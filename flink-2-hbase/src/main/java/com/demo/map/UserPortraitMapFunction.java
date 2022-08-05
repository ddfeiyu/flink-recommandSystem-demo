package com.demo.map;

import com.demo.client.HbaseClient;
import com.demo.client.MysqlClient;
import com.demo.domain.LogEntity;
import com.demo.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;

/**
 * * FIXME  : 画像任务 ====> 用户画像记录 -> 实现基于标签的推荐逻辑
 *  *  *  * FIXME :  数据存储在Hbase用户画像表  ====> user 表
 *  *
 *  *  FIXME  : v1.0按照三个维度去计算用户画像,分别是用户的颜色兴趣,用户的产地兴趣,和用户的风格兴趣.
 *  *  FIXME  : 根据日志不断的修改用户画像的数据,记录在Hbase中.
 * @author XINZE
 */
public class UserPortraitMapFunction implements MapFunction<String, String> {
    @Override
    public String map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);
        // 查询产品基本信息表： select  * from product where product_id = %s
        ResultSet rst = MysqlClient.selectById(log.getProductId());
        if (rst != null){
            while (rst.next()){
                String userId = String.valueOf(log.getUserId());
                /**
                 *   * @param tablename 表名   : user
                 *      * @param rowkey 行号   : userId
                 *      * @param famliyname 列族名 : country
                 *      * @param column 列名    : country
                 */
                String country = rst.getString("country");
                HbaseClient.increamColumn("user",userId,"country",country);

                /**
                 *   * @param tablename 表名   : user
                 *      * @param rowkey 行号   : userId
                 *      * @param famliyname 列族名 : color
                 *      * @param column 列名    : color
                 */
                String color = rst.getString("color");
                HbaseClient.increamColumn("user",userId,"color",color);

                /**
                 *   * @param tablename 表名   : user
                 *      * @param rowkey 行号   : userId
                 *      * @param famliyname 列族名 : style
                 *      * @param column 列名    : style
                 */
                String style = rst.getString("style");
                HbaseClient.increamColumn("user",userId,"style",style);
            }

        }
        return null;
    }
}
