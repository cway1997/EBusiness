package com.cway.ebusiness.util;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 22:34 2019/8/19
 */
public class SparkUtils {

    public static void setMaster(SparkConf conf){
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            conf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据
     *
     * @param sc
     * @param sparkSession
     */
    public static void mockData(JavaSparkContext sc, SparkSession sparkSession) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sparkSession);
        }
    }

    /**
     * 获取指定日期内的用户访问行为
     *
     * @param sparkSession sparkSession
     * @param taskParam    筛选维度(参数)
     * @return RowRDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(
            SparkSession sparkSession, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * " +
                "from user_visit_action " +
                "where date>='" + startDate + "'" +
                "and date<='" + endDate + "'";
        Dataset<Row> actionDS = sparkSession.sql(sql);
        return actionDS.javaRDD();
    }

}
