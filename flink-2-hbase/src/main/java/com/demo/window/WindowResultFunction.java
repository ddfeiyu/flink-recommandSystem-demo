package com.demo.window;

import com.demo.domain.TopProductEntity;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * FIXME: 聚合操作.aggregate(AggregateFunction af, WindowFunction wf)
 * 的第二个参数WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出。
 *
 * 我们这里实现的WindowResultFunction将<主键商品ID，窗口，点击量>封装成了ItemViewCount进行输出。
 *
 * FIXME  WindowFunction< IN, OUT, KEY, W extends Window >  其中 TopProductEntity 是 输出，即 商品点击量(窗口操作的输出类型)
 */
public class WindowResultFunction implements WindowFunction<Long, TopProductEntity, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> aggregateResult, Collector<TopProductEntity> collector) throws Exception {
		int itemId = key.getField(0);

		// 每个商品的点击量
		Long count = aggregateResult.iterator().next();
        collector.collect(TopProductEntity.of(itemId,window.getEnd(),count));
    }
}
