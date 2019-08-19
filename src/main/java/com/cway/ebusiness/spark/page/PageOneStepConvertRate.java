package com.cway.ebusiness.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.util.ParamUtils;
import com.cway.ebusiness.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 20:31 2019/8/18
 */
public class PageOneStepConvertRate {

    public static void main(String[] args) {
        // 1.构建spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2.生成模拟数据
        SparkUtils.mockData(sc, sparkSession);

        // 3.查询任务，获取任务的参数
        long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 4.查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sparkSession, taskParam);


    }




}
