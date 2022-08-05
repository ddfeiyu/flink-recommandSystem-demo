package com.demo.map;

import com.demo.client.HbaseClient;
import com.demo.client.MysqlClient;
import com.demo.domain.LogEntity;
import com.demo.util.AgeUtil;
import com.demo.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;

/**
 *   : 画像任务 ====> 产品画像记录 -> 实现基于标签的推荐逻辑
 *  :  数据存储在Hbase产品画像表  ====> prod表
 *
 * @author XINZE
 */
public class ProductPortraitMapFunction implements MapFunction<String, String> {
    @Override
    public String map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);
        // FIXME :  查询mysql用户基本信息表： select  * from user where user_id = %s
        ResultSet rst = MysqlClient.selectUserById(log.getUserId());
        if (rst != null){
            while (rst.next()){
                /**
                 *   * @param tablename 表名   : prod
                 *      * @param rowkey 行号   : productId
                 *      * @param famliyname 列族名 : sex
                 *      * @param column 列名    : sex
                 */
                String productId = String.valueOf(log.getProductId());
                String sex = rst.getString("sex");
                HbaseClient.increamColumn("prod",productId,"sex",sex);

                /**
                 *   * @param tablename 表名   : prod
                 *      * @param rowkey 行号   : productId
                 *      * @param famliyname 列族名 : age
                 *      * @param column 列名    : age
                 */
                String age = rst.getString("age");
                HbaseClient.increamColumn("prod",productId,"age", AgeUtil.getAgeType(age));
            }
        }
        return null;
    }
}
