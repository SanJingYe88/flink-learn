package it.com.function.ds;

import it.com.entity.ViewTimeLenEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 生成浏览时长数据
 */
public class ViewTimeLenEventSource implements SourceFunction<ViewTimeLenEvent> {

    /**
     * 声明一个标志位
     */
    private Boolean running = true;

    @Override
    public void run(SourceContext<ViewTimeLenEvent> ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        int[] ids = {1, 2, 3, 4};
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./prod?id=100", "./prod?id=10", "./fav"};

        //循环生成数据
        while (running) {
            int i = random.nextInt(users.length);
            int id = ids[i];
            String user = users[i];
            String url = urls[random.nextInt(urls.length)];
            int viewTimeLen = random.nextInt(100);
            long timestamp = System.currentTimeMillis();
            ctx.collect(new ViewTimeLenEvent(id, user, url, viewTimeLen, timestamp));
            // 生产频率慢一点
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}