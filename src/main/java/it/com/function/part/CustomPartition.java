package it.com.function.part;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区规则,这里入参的 IN 类型，是和 CustomKeySelector 返回的类型保持一致的
 */
public class CustomPartition implements Partitioner<Integer> {

    /**
     * 分区规则
     * @param key 需要分区的键
     * @param numPartitions 总共的分区数量
     * @return 返回一个整数，表示分区索引。这个索引应该在0到 numPartitions - 1 的范围内。
     */
    @Override
    public int partition(Integer key, int numPartitions) {
        // 根据 分区 key % 2 来分区
        return key % 2;
    }
}
