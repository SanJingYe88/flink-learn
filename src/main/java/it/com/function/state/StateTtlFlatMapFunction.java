package it.com.function.state;

import it.com.entity.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 展示 State TTL 的可配置项
 */
public class StateTtlFlatMapFunction extends RichFlatMapFunction<Event, String> {

    /**
     * 定义状态
     */
    private ValueState<Event> eventValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取状态的运行上下文，方便调用状态等等。
        ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-value", Types.POJO(Event.class));
        eventValueState = getRuntimeContext().getState(valueStateDescriptor);

        // 配置状态的TTL
        // 一旦设置了 TTL，那么如果上次访问的时间戳 + TTL超过了当前时间，则表明状态过期了。
        StateTtlConfig ttlConfig = StateTtlConfig
                // 状态有效时间
                .newBuilder(Time.seconds(10))
                // 选择时间特性
                .setTtlTimeCharacteristic(
                        // StateTtlConfig.TtlTimeCharacteristic.EventTime  // 事件时间 1.16版本以及之前
                        // 或
                        StateTtlConfig.TtlTimeCharacteristic.ProcessingTime  // 处理时间
                )
                // 设置状态时间戳更新时机
                /**
                 * UpdateType 表示状态时间戳的更新的时机（延长状态有效期），是一个 Enum 对象。
                 * 如果设置为 Disabled，则表明不更新时间戳；
                 * 如果设置为 OnCreateAndWrite，则表明当状态创建或每次写入时都会更新时间戳；
                 * 如果设置为 OnReadAndWrite，在状态创建、写入、读取均会更新状态的时间戳。
                 */
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 过期状态清理策略
                // CleanupStrategies
                // .cleanupIncrementally() 增量清理
                /**
                 * 针对 Heap StateBackend 使用的策略为：CleanupStrategies.Strategies.INCREMENTAL_CLEANUP，对应 IncrementalCleanupStrategy 类
                 * 针对 RocksDB StateBackend 使用的策略为：CleanupStrategies.Strategies.ROCKSDB_COMPACTION_FILTER，对应 RocksdbCompactFilterCleanupStrategy 类
                 *
                 * 对于增量清理功能，Flink 可以被配置为每读取若干条记录就执行一次清理操作，而且可以指定每次要清理多少条失效记录；
                 * 对于 RocksDB 的状态清理，则是通过 JNI 来调用 C++ 语言编写的 FlinkCompactionFilter 来实现，底层是通过 RocksDB 提供的后台 Compaction 操作来实现对失效状态过滤的。
                 */
                // .cleanupFullSnapshot() 全量清理
                /**
                 * 使用的策略为：CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT，对应的是 EmptyCleanupStrategy 类，表示对过期状态不做主动清理。
                 * 当执行完整快照（Snapshot / Checkpoint）时，会生成一个较小的状态文件，但本地状态并不会减小。
                 * 唯有当作业重启并从上一个快照点恢复后，本地状态才会实际减小，因此可能仍然不能解决内存压力的问题。
                 */
                .cleanupFullSnapshot()
                .build();
        /**
         * 如果过期的用户值尚未被清理，则返回该值。ReturnExpiredIfNotCleanedUp
         * 永远不返回过期的用户值。NeverReturnExpired
         */

        // 启用状态过期
        valueStateDescriptor.enableTimeToLive(ttlConfig);
    }

    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {

    }
}