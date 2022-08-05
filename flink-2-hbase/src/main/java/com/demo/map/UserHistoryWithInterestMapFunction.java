
package com.demo.map;

import com.demo.client.HbaseClient;
import com.demo.domain.LogEntity;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * FIXME  :上下文任务
 * * FIXME  : 实现基于上下文的推荐逻辑
 *  *  * FIXME :  数据存储在Hbase 用户兴趣表表  ====>   u_interest表
 *
 *  FIXME  : 根据用户对同一个产品的操作计算兴趣度,计算规则通过操作间隔时间(如购物 - 浏览 < 100s)则判定为一次兴趣事件
 *  FIXME  : 通过Flink的 ValueState 实现,如果用户的操作Action=3(收藏),则清除这个产品的state, 如果超过100s没有出现Action=3的事件,也会清除这个state
 *  FIXME  : 数据存储在Hbase的 u_interest 表
 *  *  *
 * @author XINZE
 */
public class UserHistoryWithInterestMapFunction extends RichMapFunction<LogEntity, String> {

    // Action : 动作类, 记录动作类型和动作发生时间(Event Time)
    ValueState<Action> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        // 设置 state 的过期时间为100s
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(100L))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Action> desc = new ValueStateDescriptor<>("Action time", Action.class);
        desc.enableTimeToLive(ttlConfig);
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public String map(LogEntity logEntity) throws Exception {
        // 上次动作时间
        Action actionLastTime = state.value();
        Action actionThisTime = new Action(logEntity.getAction(), logEntity.getTime().toString());
        int times = 1;
        // 如果用户没有操作 则为state创建值
        if (actionLastTime == null) {
            actionLastTime = actionThisTime;
            /**
             * 保存用户兴趣到 Hbase表 u_interest
             */
            saveToHBase(logEntity, 1);
        }else{
            // action: 1 -> 浏览  2 -> 分享  3 -> 购物
            // 计算两次行动的跨度次数，如 从1(浏览)到3(购物)，则为2次跨度
            times = getTimesByRule(actionLastTime, actionThisTime);
        }
        /**
         * 保存用户兴趣到 Hbase表 u_interest
         */
        saveToHBase(logEntity, times);

        // 如果用户的操作为3(购物),则清除这个key的state
        //  *  FIXME  : 通过Flink的 ValueState 实现,如果用户的操作Action=3(收藏),则清除这个产品的state, 如果超过100s没有出现Action=3的事件,也会清除这个state
        if (actionThisTime.getType().equals("3")){
            state.clear();
        }
        return null;
}

    /**
     * 两次行动的跨度
     * @param actionLastTime
     * @param actionThisTime
     * @return
     */
    private int getTimesByRule(Action actionLastTime, Action actionThisTime) {
        // 动作主要有3种类型
        // FIXME: 1 -> 浏览  2 -> 分享  3 -> 购物
        int a1 = Integer.parseInt(actionLastTime.getType());
        int a2 = Integer.parseInt(actionThisTime.getType());
        int t1 = Integer.parseInt(actionLastTime.getTime());
        int t2 = Integer.parseInt(actionThisTime.getTime());
        int plus = 1;
        // 如果动作连续发生且时间很短(小于100秒内完成动作), 则标注为用户对此产品兴趣度很高
        // 计算规则通过操作间隔时间(如购物 - 浏览 < 100s)则判定为一次兴趣事件
        if (a2 > a1 && (t2 - t1) < 100_000L){
            plus *= a2 - a1;
        }
        return plus;
    }

    private void saveToHBase(LogEntity log, int times) throws Exception {
        if (log != null){
            for (int i = 0; i < times; i++) {

                /**
                 *   * @param tablename 表名   : u_interest
                 *      * @param rowkey 行号   : user_id
                 *      * @param famliyname 列族名 : p
                 *      * @param column 列名    : product_id
                 */
                HbaseClient.increamColumn("u_interest",String.valueOf(log.getUserId()),"p",String.valueOf(log.getProductId()));
            }
        }
    }

}

/**
 * 动作类 记录动作类型和动作发生时间(Event Time)
 */
class Action implements Serializable
{

    private String type;
    private String time;

    public Action(String type, String time) {
        this.type = type;
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}