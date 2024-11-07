package it.com.launcher.join;

import it.com.entity.Order;
import it.com.entity.Payment;
import it.com.enums.OrderStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 匹配订单支付成功
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 订单流
        DataStream<Order> orderStream = env.fromElements(
                new Order("order1", 1000L, OrderStatus.UNPAID, 100.0),
                new Order("order2", 2000L, OrderStatus.UNPAID, 200.0),
                new Order("order3", 3000L, OrderStatus.UNPAID, 300.0)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
        );

        // 支付流
        DataStream<Payment> paymentStream = env.fromElements(
                new Payment("order1", 1500L, 100.0),  // order1 会join成功
                new Payment("order4", 2500L, 400.0)   // order4 找不到对应订单，join失败
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Payment>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((payment, timestamp) -> payment.getPayTime())
        );

        // 执行interval join
        orderStream.keyBy(Order::getOrderId)
                .intervalJoin(paymentStream.keyBy(Payment::getOrderId))
                .between(Time.seconds(-5), Time.seconds(5))  // 订单时间前后5秒内的支付都认为有效
                .process(new ProcessJoinFunction<Order, Payment, String>() {
                    @Override
                    public void processElement(Order order, Payment payment, Context ctx, Collector<String> out) {
                        // 只有join成功的才会到这里
                        out.collect(String.format("订单 %s 在 %d 时刻支付成功，支付时间 %d", order.getOrderId(), order.getCreateTime(), payment.getPayTime()));
                    }
                })
                .print();

        env.execute("Interval Join Demo");
    }
}
