package com.cway.ebusiness.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.util.ParamUtils;
import com.cway.ebusiness.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 22:10 2019/8/21
 */
public class AreaTop3Product {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AreaTop3Product");
        SparkUtils.setMaster(conf);

        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        // 注册自定义函数
        sparkSession.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sparkSession.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());

        SparkUtils.mockData(sc, sparkSession);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskId);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 查询用户指定日期范围的RDD
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = getCityId2ClickActionRDDByDate(sparkSession, startDate, endDate);

        JavaPairRDD<Long, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(sparkSession);

        generateTempClickProductBasicTable(sparkSession, cityId2ClickActionRDD, cityId2CityInfoRDD);

        /**
         * UDF: concat2()
         * UDAF: group_concat_distinct()
         */

        sc.close();
//        sparkSession.close();
    }

    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDDByDate(SparkSession sparkSession,
                                                                         String startDate, String endDate) {
        String sql = "select city_id,click_product_id product_id" +
                "from user_visit_action" +
                "where click_product_id is not null" +
                "and click_product_id != 'null'" +
                "and click_product_id != 'NULL'" +
                "and action_time >='" + startDate + "'" +
                "and action_time <='" + endDate + "'";

        Dataset<Row> clickActionDF = sparkSession.sql(sql);
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = clickActionRDD.mapToPair((PairFunction<Row, Long, Row>) row -> new Tuple2<>(row.getLong(0), row));
        return cityId2ClickActionRDD;
    }

    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SparkSession sparkSession) {
        // 构建mysql连接配置信息
        String url = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
        }

        Map<String, String> options = new HashMap<>(2);
        options.put("url", url);
        options.put("dbtable", "city_info");

        Dataset<Row> cityInfoDF = sparkSession.read().format("jdbc").options(options).load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair((PairFunction<Row, Long, Row>) row -> new Tuple2<>(row.getLong(0), row));
        return cityId2CityInfoRDD;
    }

    private static void generateTempClickProductBasicTable(
            SparkSession sparkSession,
            JavaPairRDD<Long, Row> cityId2ClickActionRDD,
            JavaPairRDD<Long, Row> cityId2CityInfoRDD) {
        JavaPairRDD<Long, Tuple2<Row, Row>> joinRDD = cityId2ClickActionRDD.join(cityId2CityInfoRDD);

        JavaRDD<Row> mapRDD = joinRDD.map((Function<Tuple2<Long, Tuple2<Row, Row>>, Row>) tuple2 -> {
            long cityId = tuple2._1;
            Row clickAction = tuple2._2._1;
            Row cityInfo = tuple2._2._2;

            long productId = clickAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String area = cityInfo.getString(2);

            return RowFactory.create(cityId, cityName, area, productId);
        });

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = sparkSession.createDataFrame(mapRDD, schema);

        dataFrame.registerTempTable("tmp_clk_prod_basic_info");
//        dataFrame.createTempView();
    }

}
