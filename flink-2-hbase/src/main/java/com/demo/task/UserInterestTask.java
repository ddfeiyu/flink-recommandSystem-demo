package com.demo.task;

import com.demo.map.GetLogFunction;
import com.demo.map.UserHistoryWithInterestMapFunction;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * FIXME  :上下文任务
 * * FIXME  : 实现基于上下文的推荐逻辑
 *  *  * FIXME :  数据存储在Hbase 用户兴趣表表  ====>   u_interest表
 *
 *  FIXME  : 根据用户对同一个产品的操作计算兴趣度,计算规则通过操作间隔时间(如购物 - 浏览 < 100s)则判定为一次兴趣事件
 *  FIXME  : 通过Flink的ValueState实现,如果用户的操作Action=3(收藏),则清除这个产品的state, 如果超过100s没有出现Action=3的事件,也会清除这个state
 *  FIXME  : 数据存储在Hbase的 u_interest 表
 *  *  *
 * @author XINZE
 */
public class UserInterestTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("interest");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream
                .map(new GetLogFunction()) // new GetLogFunction(): 将kafka 的数据 转为 LogEntity类
                .keyBy("userId")  // 对userId进行分组
                /**
                 * FIXME : new UserHistoryWithInterestMapFunction()
                 * 1、根据用户对同一个产品的操作计算兴趣度,计算规则通过操作间隔时间(如购物 - 浏览 < 100s)则判定为一次兴趣事件
                 * 2、通过Flink的ValueState实现,如果用户的操作Action=3(收藏),则清除这个产品的state, 如果超过100s没有出现Action=3的事件,也会清除这个state
                 * 3、数据存储在Hbase的 u_interest 表
                 */
                .map(new UserHistoryWithInterestMapFunction());

        env.execute("User Product History");
    }
}
