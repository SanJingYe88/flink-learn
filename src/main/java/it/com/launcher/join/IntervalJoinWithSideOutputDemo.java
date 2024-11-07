package it.com.launcher.join;

import it.com.entity.Order;
import it.com.entity.Payment;
import it.com.enums.OrderStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * IntervalJoin
 * 处理那些 join 失败的事件: 使用 Side Output 输出
 */
public class IntervalJoinWithSideOutputDemo {
    private static final Logger LOG = LoggerFactory.getLogger(IntervalJoinWithSideOutputDemo.class);

    // 定义侧输出流的 OutputTag
    private static final OutputTag<Order> unmatchedOrders = new OutputTag<Order>("unmatched-orders") {
    };
    private static final OutputTag<Payment> unmatchedPayments = new OutputTag<Payment>("unmatched-payments") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建订单流
        DataStream<Order> orderStream = env.addSource(new OrderSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
        );

        // 创建支付流
        DataStream<Payment> paymentStream = env.addSource(new PaymentSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Payment>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((payment, timestamp) -> payment.getPayTime())
        );

        // 使用 CoProcessFunction 处理订单和支付的匹配
        SingleOutputStreamOperator<String> resultStream = orderStream.keyBy(Order::getOrderId)
                .connect(paymentStream.keyBy(Payment::getOrderId))
                .process(new OrderPaymentMatchFunction());

        // 处理主流（匹配成功的结果）
        resultStream.print("Matched");

        // 处理未匹配的订单
        resultStream.getSideOutput(unmatchedOrders)
                .map(order -> String.format("未匹配订单: %s, 创建时间: %d", order.getOrderId(), order.getCreateTime()))
                .print("Unmatched Orders");

        // 处理未匹配的支付
        resultStream.getSideOutput(unmatchedPayments)
                .map(payment -> String.format("未匹配支付: %s, 支付时间: %d", payment.getOrderId(), payment.getPayTime()))
                .print("Unmatched Payments");

        env.execute("Interval Join With Side Output Demo");
    }

    // 订单源
    private static class OrderSource implements SourceFunction<Order> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();

            // 发送三个订单
            ctx.collect(new Order("order1", baseTime, OrderStatus.UNPAID, 100.0));
            TimeUnit.SECONDS.sleep(1);

            ctx.collect(new Order("order2", baseTime + 5000, OrderStatus.UNPAID, 200.0));
            TimeUnit.SECONDS.sleep(1);

            ctx.collect(new Order("order3", baseTime + 10000, OrderStatus.UNPAID, 300.0));
            TimeUnit.SECONDS.sleep(1);

            // 等待足够长的时间以确保某些订单会超时
            TimeUnit.SECONDS.sleep(10);
            isRunning = false;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 支付源
    private static class PaymentSource implements SourceFunction<Payment> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Payment> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();

            // 延迟2秒后开始发送支付数据
            TimeUnit.SECONDS.sleep(2);

            // 发送两个支付，其中一个没有对应的订单
            ctx.collect(new Payment("order1", baseTime + 2000, 100.0));  // order1的支付会匹配成功
            TimeUnit.SECONDS.sleep(1);

            ctx.collect(new Payment("order4", baseTime + 7000, 400.0));  // order4没有对应订单

            TimeUnit.SECONDS.sleep(10);
            isRunning = false;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 订单支付匹配处理函数
    private static class OrderPaymentMatchFunction extends CoProcessFunction<Order, Payment, String> {
        private ValueState<Order> orderState;
        private ValueState<Payment> paymentState;
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<>("order", Order.class));
            paymentState = getRuntimeContext().getState(new ValueStateDescriptor<>("payment", Payment.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement1(Order order, Context ctx, Collector<String> out) throws Exception {
            Payment payment = paymentState.value();
            if (payment != null) {
                // 找到匹配的支付，输出匹配成功的结果
                out.collect(String.format("订单 %s 匹配到支付，订单时间：%d，支付时间：%d", order.getOrderId(), order.getCreateTime(), payment.getPayTime()));
                // 清除已匹配的支付状态
                paymentState.clear();
                if (timerState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                    timerState.clear();
                }
            } else {
                // 暂存订单，等待支付
                orderState.update(order);
                // 设置5秒后的定时器
                long timer = order.getCreateTime() + 5000;
                ctx.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);
                LOG.info("订单 {} 等待支付，将在 {} 超时", order.getOrderId(), timer);
            }
        }

        @Override
        public void processElement2(Payment payment, Context ctx, Collector<String> out) throws Exception {
            Order order = orderState.value();
            if (order != null) {
                // 找到匹配的订单，输出匹配成功的结果
                out.collect(String.format("支付 %s 匹配到订单，订单时间：%d，支付时间：%d", payment.getOrderId(), order.getCreateTime(), payment.getPayTime()));
                // 清除已匹配的订单状态
                orderState.clear();
                if (timerState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerState.value());
                    timerState.clear();
                }
            } else {
                // 暂存支付���等待订单
                paymentState.update(payment);
                // 设置5秒后的定时器
                long timer = payment.getPayTime() + 5000;
                ctx.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);
                LOG.info("支付 {} 等待订单，将在 {} 超时", payment.getOrderId(), timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，说明超时未匹配
            Order order = orderState.value();
            Payment payment = paymentState.value();

            if (order != null) {
                // 将未匹配的订单输出到侧输出流
                ctx.output(unmatchedOrders, order);
                LOG.info("订单 {} 超时未匹配支付", order.getOrderId());
                orderState.clear();
            }

            if (payment != null) {
                // 将未匹配的支付输出到侧输出流
                ctx.output(unmatchedPayments, payment);
                LOG.info("支付 {} 超时未匹配订单", payment.getOrderId());
                paymentState.clear();
            }

            timerState.clear();
        }
    }
}