package com.demo.top;

import com.demo.domain.TopProductEntity;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *  FIXME : 求点击量前3名的商品
 * ProcessFunction是Flink提供的一个low-level API，用于实现更高级的功能。
 *
 * 它主要提供了 定时器timer的功能（支持 EventTime 或 ProcessingTime）。
 *
 * 本案例中我们将利用timer来判断何时收齐了某个window下所有商品的点击量数据。
 *
 * 由于Watermark的进度是全局的，
 *
 * 在processElement方法中，每当收到一条数据ItemViewCount，
 * 我们就注册一个windowEnd+1的定时器（Flink框架会自动忽略同一时间的重复注册）。
 *
 *
 * windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的Watermark，即收齐了该windowEnd下的所有商品窗口统计值。
 *
 * 我们在onTimer()中处理将收集的所有商品及点击量进行排序，选出TopN，并将排名信息格式化成字符串后进行输出。
 *
 * FIXME ：KeyedProcessFunction<K, I, O> ，其中 输入是 TopProductEntity即每个窗口的点击量的数据流，输出是 List<String>
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, TopProductEntity, List<String>>  {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    private ListState<TopProductEntity> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<TopProductEntity> itemsStateDesc = new ListStateDescriptor<>(
                "itemState-state",
                TopProductEntity.class);
        itemState = getRuntimeContext().getListState(itemsStateDesc);
    }

    /**
     * * 在processElement方法中，每当收到一条数据ItemViewCount，
     *  * 我们就注册一个windowEnd+1的定时器（Flink框架会自动忽略同一时间的重复注册）。
     *    * windowEnd+1的定时器被触发时，意味着收到了windowEnd+1的Watermark，即收齐了该windowEnd下的所有商品窗口统计值。
     * @param topProductEntity
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(TopProductEntity topProductEntity, Context context, Collector<List<String>> collector) throws Exception {
        itemState.add(topProductEntity);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        context.timerService().registerEventTimeTimer(topProductEntity.getWindowEnd() + 1);
    }


    /**
     * 在onTimer()中处理将收集的所有商品及点击量进行排序，选出TopN，并将排名信息格式化成字符串后进行输出。
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<String>> out) throws Exception {
        List<TopProductEntity> allItems = new ArrayList<>();
        for (TopProductEntity item : itemState.get()) {
            allItems.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<TopProductEntity>() {
            @Override
            public int compare(TopProductEntity o1, TopProductEntity o2) {
                return (int) (o2.getActionTimes() - o1.getActionTimes());
            }
        });
        List<String> ret = new ArrayList<>();
        allItems.forEach(i-> ret.add(String.valueOf(i.getProductId())));
        out.collect(ret);
    }
}
