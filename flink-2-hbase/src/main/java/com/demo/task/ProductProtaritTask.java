package com.demo.task;

import com.demo.map.ProductPortraitMapFunction;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 产品画像 -> Hbase
 *  * FIXME  : 画像任务 ====> 产品画像记录 -> 实现基于标签的推荐逻辑
 *  * FIXME :  数据存储在Hbase产品画像表  ====> prod表
 *  *
 * @author XINZE
 */
public class ProductProtaritTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("ProductPortrait");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream.map(new ProductPortraitMapFunction());
        env.execute("Product Portrait");

    }
}
