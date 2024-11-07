package it.com.launcher.input;

import it.com.util.DataGeneratorSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 从文件中读取数据 TestReadFileLauncher
 */
@Slf4j
public class TestReadFileLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从指定文件获取 FileSource
        FileSource<String> fileSource = DataGeneratorSourceUtil.readFile("D:\\code\\source\\flink-learn\\src\\main\\resources\\source\\test.txt");
        // 加载 FileSource
        DataStreamSource<String> readFileSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
        readFileSource.print();
        env.execute();
    }
}
