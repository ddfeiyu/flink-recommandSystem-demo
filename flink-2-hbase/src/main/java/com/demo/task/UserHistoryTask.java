package com.demo.task;

import com.demo.map.UserHistoryMapFunction;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * FIXME :协同过滤任务
 * FIXME: 记录用户-产品关联性 =======> 数据存储在 Hbase 表 p_history
 * FIXME: 记录产品-用户关联性 =======> 数据存储在 Hbase 表 u_history
 *
 */
public class UserHistoryTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("history");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream.map(new UserHistoryMapFunction());

        env.execute("User Product History");
    }
}
