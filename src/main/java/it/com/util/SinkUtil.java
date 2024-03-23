package it.com.util;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class SinkUtil {

    public static FileSink<String> sinkToFile() {
        FileSink<String> fileSink = FileSink
                // 指定输出方式 行式输出、文件路径、编码
                .<String>forRowFormat(new Path("D:\\code\\source\\flink-learn\\src\\main\\resources\\data\\output"), new SimpleStringEncoder<String>("UTF-8"))
                // 指定输出文件的名称配置 前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("flink") // 指定前缀
                                .withPartSuffix(".txt")   // 指定后缀
                                .build()
                )
                // 按照时间进行目录分桶：每分钟生成一个目录，目录格式为 yyyy-MM-dd HH-mm
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm", ZoneId.systemDefault()))
                // 文件滚动策略:  10分钟 或 1m 生成新的文件
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofHours(1))
                                .withMaxPartSize(new MemorySize(1024 * 1024 * 1024))
                                .build()
                ).build();
        return fileSink;
    }
}
