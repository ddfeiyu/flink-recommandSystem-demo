package com.demo.map;

import com.demo.client.HbaseClient;
import com.demo.domain.LogEntity;
import com.demo.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author XINZE
 */
public class UserHistoryMapFunction implements MapFunction<String, String> {

    /**
     * 将 用户-产品  和 产品-用户 分别存储Hbase表
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public String map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);
        if (null != log){
            /**
             *   * @param tablename 表名   : u_history
             *      * @param rowkey 行号   : user_id
             *      * @param famliyname 列族名 : p
             *      * @param column 列名    : product_id
             */
            HbaseClient.increamColumn("u_history",String.valueOf(log.getUserId()),"p",String.valueOf(log.getProductId()));

            /**
             *   * @param tablename 表名   : p_history
             *      * @param rowkey 行号   : product_id
             *      * @param famliyname 列族名 : p
             *      * @param column 列名    : user_id
             */
            HbaseClient.increamColumn("p_history",String.valueOf(log.getProductId()),"p",String.valueOf(log.getUserId()));
        }
        return "";
    }
}
