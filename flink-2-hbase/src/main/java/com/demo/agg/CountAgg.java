package com.demo.agg;

import com.demo.domain.LogEntity;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * FIXME : 它能使用AggregateFunction提前聚合掉数据，减少state的存储压力。
 * FIXME : 较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，最后一起计算要高效地多。
 *
 * FIXME: 这里的CountAgg实现了AggregateFunction接口，功能是统计窗口中的条数，即遇到一条数据就加一。
 */
public class CountAgg implements AggregateFunction<LogEntity, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LogEntity logEntity, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
