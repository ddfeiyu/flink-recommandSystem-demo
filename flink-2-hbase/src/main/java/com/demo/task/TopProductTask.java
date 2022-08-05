package com.demo.task;

import com.demo.agg.CountAgg;
import com.demo.domain.LogEntity;
import com.demo.domain.TopProductEntity;
import com.demo.map.TopProductMapFunction;
import com.demo.sink.TopNRedisSink;
import com.demo.top.TopNHotItems;
import com.demo.util.Property;
import com.demo.window.WindowResultFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 *
 * FIXME : 实时热度榜任务 -> 实现基于热度的推荐逻辑
 * FIXME : 热门商品 -> redis
 *
 * * FIXME : 通过Flink时间窗口机制,统计当前时间的实时热度,并将数据缓存在Redis中.
 * * FIXME : 通过Flink的窗口机制计算实时热度,使用ListState保存一次热度榜
 * * FIXME : 数据存储在redis中,按照时间戳存储list
 * @author XINZE
 */
public class TopProductTask {

    private static final int topSize = 5;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
				.setHost(Property.getStrValue("redis.host"))
//				.setPort(Property.getIntValue("redis.port"))
//				.setDatabase(Property.getIntValue("redis.db"))
				.build();

        Properties properties = Property.getKafkaProperties("topProuct");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));

        // 自定义MapFunction： new TopProductMapFunction(): 将kafka 的数据 转为 LogEntity 类
        DataStream<TopProductEntity> topProduct = dataStream.map(new TopProductMapFunction()).
                // 抽取时间戳做watermark 以 秒 为单位
                assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogEntity>() {
                    @Override
                    public long extractAscendingTimestamp(LogEntity logEntity) {
                        return logEntity.getTime() * 1000;
                    }
                })

                // FIXME  第一步： 设置滑动窗口，统计点击量 ==========>  得到每个商品在每个窗口的点击量的数据流。

                // 按照productId 按滑动窗口
                .keyBy("productId")
                /**
                 * FIXME  : 由于要每隔5秒钟统计一次最近一分钟每个商品的点击量，所以窗口大小是一分钟，每隔5秒钟滑动一次。
                 * 即分别要统计[09:00:00, 09:01:00), [09:00:05, 09:01:05), [09:00:10, 09:01:10)…等窗口的商品点击量。
                 * 是一个常见的滑动窗口需求（Sliding Window）
                 */
                .timeWindow(Time.seconds(60),Time.seconds(5))
                /**
                 * FIXME : 然后我们使用 .aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作，
                 * 它能使用AggregateFunction提前聚合掉数据，减少state的存储压力。
                 *
                 * 较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，最后一起计算要高效地多。
                 *
                 */
                /**
                 * FIXME: 这里的CountAgg实现了AggregateFunction接口，功能是统计窗口中的条数，即遇到一条数据就加一。
                 */
                /**
                 * FIXME: 聚合操作.aggregate(AggregateFunction af, WindowFunction wf)
                 * 的第二个参数WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出。
                 *
                 * 我们这里实现的WindowResultFunction将<主键商品ID，窗口，点击量>封装成了ItemViewCount进行输出。
                 */
                // FIXME : 现在我们就得到了每个商品在每个窗口的点击量的数据流。
                .aggregate(new CountAgg(), new WindowResultFunction())



                // FIXME  第二步： 计算每个滑动窗口最热门的商品

                /**
                 * 为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组，这里根据ItemViewCount中的windowEnd进行keyBy()操作。
                 *
                 * 然后使用 ProcessFunction 实现一个自定义的TopN函数 TopNHotItems 来计算点击量排名前3名的商品，并将排名结果格式化成字符串，便于后续输出。
                 */
                .keyBy("windowEnd")  // 注意： 这里的windowEnd是TopProductEntity对象的字段 windowEnd

                // 求点击量前5名的商品
                .process(new TopNHotItems(topSize)).flatMap(new FlatMapFunction<List<String>, TopProductEntity>() {
                    @Override
                    public void flatMap(List<String> strings, Collector<TopProductEntity> collector) throws Exception {
                        System.out.println("-------------Top N Product------------");
                        for (int i = 0; i < strings.size(); i++) {
                            TopProductEntity top = new TopProductEntity();
                            // 排名
                            top.setRankName(String.valueOf(i));
                            // 商品
                            top.setProductId(Integer.parseInt(strings.get(i)));
                            // 输出排名结果
                            System.out.println(top);
                            collector.collect(top);
                        }
                    }
                });
                // 存储topN数据到redis
        topProduct.addSink(new RedisSink<>(flinkJedisPoolConfig,new TopNRedisSink()));

        env.execute("Top N ");
    }
}
