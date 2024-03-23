package it.com.function;

import it.com.entity.SanGuoUser;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流规则
 * <p>
 * ProcessFunction：参数1：输入类型。参数2：输出类型
 */
public class CountryProcessFunction extends ProcessFunction<SanGuoUser, SanGuoUser> {

    // 定义分类标签
    public static OutputTag<SanGuoUser> WeiTag = new OutputTag<SanGuoUser>("Wei") {
    };
    public static OutputTag<SanGuoUser> ShuTag = new OutputTag<SanGuoUser>("Shu") {
    };
    public static OutputTag<SanGuoUser> WuTag = new OutputTag<SanGuoUser>("Wu") {
    };

    @Override
    public void processElement(SanGuoUser value, Context ctx, Collector<SanGuoUser> out) throws Exception {
        String country = value.getCountry();
        if ("魏国".equals(country)) {
            ctx.output(WeiTag, value);
        } else if ("蜀国".equals(country)) {
            ctx.output(ShuTag, value);
        } else if ("吴国".equals(country)) {
            ctx.output(WuTag, value);
        } else {
            out.collect(value);
        }
    }
}
