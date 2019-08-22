package com.cway.ebusiness.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.IAreaTop3ProductDAO;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.AreaTop3Product;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.util.ParamUtils;
import com.cway.ebusiness.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        // shuffle并行度,默认200
//        sparkSession.conf().set("spark.sql.shuffle.partition", "1000");
        // Spark SQL内置的map join，阈值默认1G
//        sparkSession.conf().set("spark.sql.autoBroadcastJoinThreshold", "20971520");
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());


        // 注册自定义函数
        sparkSession.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sparkSession.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
        sparkSession.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
        sparkSession.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        sparkSession.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);

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

        generateTempAreaProductClickCountTable(sparkSession);

        generateTempAreaFullProductClickCountTable(sparkSession);

        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sparkSession);

        List<Row> rows = areaTop3ProductRDD.collect();

        persistAreaTop3ProductRDD(taskId, rows);

        sc.close();
//        sparkSession.close();
    }

    private static void testDriverHA(){

    }

    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDDByDate(SparkSession sparkSession,
                                                                         String startDate, String endDate) {
        String sql =
                "SELECT " +
                        "city_id," +
                        "click_product_id product_id " +
                        "FROM user_visit_action " +
                        "WHERE click_product_id IS NOT NULL " +
//                        "AND click_product_id != 'null' " +
//                        "AND click_product_id != 'NULL' " +
                        "AND date >='" + startDate + "' " +
                        "AND date <='" + endDate + "'";

        Dataset<Row> clickActionDF = sparkSession.sql(sql);
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = clickActionRDD.mapToPair((PairFunction<Row, Long, Row>) row -> new Tuple2<>(row.getLong(0), row));
        return cityId2ClickActionRDD;
    }

    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SparkSession sparkSession) {
        // 构建mysql连接配置信息
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<>(4);
        options.put("url", url);
        options.put("user", user);
        options.put("password", password);
        options.put("dbtable", "city_info");

        Dataset<Row> cityInfoDF = sparkSession.read().format("jdbc").options(options).load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair((PairFunction<Row, Long, Row>) row -> new Tuple2<>((long) row.getInt(0), row));
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

        dataFrame.createOrReplaceTempView("tmp_click_product_basic");
//        dataFrame.createTempView();
    }

    /**
     * 生成各区域各商品点击次数临时表
     *
     * @param sparkSession
     */
    private static void generateTempAreaProductClickCountTable(SparkSession sparkSession) {
        String sql =
                "SELECT " +
                        "area," +
                        "product_id," +
                        "count(*) click_count," +
                        "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                        "FROM tmp_click_product_basic " +
                        "GROUP BY area,product_id";

        // 双重GROUP BY
/*        String _sql =
                "SELECT " +
                    "product_id_area," +
                    "count(click_count) click_count," +
                    "group_concat_distinct(city_infos) " +
                "FROM (" +
                    "SELECT " +
                        "remove_random_prefix(product_id_area) product_id_area," +
                        "click_count," +
                        "city_infos " +
                    "FROM (" +
                        "SELECT " +
                            "product_id_area," +
                            "count(*) click_count," +
                            "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                        "FROM (" +
                            "SELECT " +
                                "random_prefix(concat_long_string(product_id,area,':'),10) product_id_area," +
                                "city_id," +
                                "city_name " +
                            "FROM tmp_click_product_basic " +
                        ") t1 " +
                        "GROUP BY product_id_area " +
                    ") t2 " +
                ") t3 " +
                "GROUP BY product_id_area";*/

        Dataset<Row> dataFrame = sparkSession.sql(sql);

        dataFrame.createOrReplaceTempView("tmp_area_product_click_count");
    }

    /**
     * 生成区域商品点击临时表(包含完整商品信息)
     * area,product_id,click_count,city_infos,product_name,product_status
     *
     * @param sparkSession
     */
    private static void generateTempAreaFullProductClickCountTable(SparkSession sparkSession) {
        String sql =
                "SELECT " +
                        "tapcc.area," +
                        "tapcc.product_id," +
                        "tapcc.click_count," +
                        "tapcc.city_infos," +
                        "pi.product_name," +
                        "if(get_json_object(pi.extend_info,'product_status')='0','自营商品','第三方商品') product_status " +
                        "FROM tmp_area_product_click_count tapcc " +
                        "JOIN product_info pi ON tapcc.product_id=pi.product_id";

        // 随机key和扩容
/*        JavaRDD<Row> rdd = sparkSession.sql("select * from product_info").javaRDD();
        JavaRDD<Row> flattedRDD = rdd.flatMap((FlatMapFunction<Row, Row>) row -> {
            List<Row> list = new ArrayList<>();
            long productId = row.getLong(0);
            for (int i = 0; i < 10; i++) {
                String _productId = i + "_" + productId;
                Row _row = RowFactory.create(_productId, row.get(1), row.get(2));
                list.add(_row);
            }
            return list.iterator();
        });

        StructType _schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.StringType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> _dataFrame = sparkSession.createDataFrame(flattedRDD, _schema);
        _dataFrame.createOrReplaceTempView("tmp_product_info");

        String _sql =
                "SELECT " +
                    "tapcc.area" +
                    "remove_random_prefix(tapcc.product_id) product_id," +
                    "tapcc.click_count," +
                    "tapcc.city_infos," +
                    "pi.product_name," +
                    "if(get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品') product_status " +
                "FROM (" +
                    "SELECT " +
                        "area," +
                        "random_prefix(product_id) product_id," +
                        "click_count," +
                        "city_infos " +
                    "FROM tmp_area_product_click_count " +
                ") tapcc " +
                "JOIN tmp_product_info pi ON tapcc.product_id=pi.product_id";*/

        Dataset<Row> dataFrame = sparkSession.sql(sql);
        dataFrame.createOrReplaceTempView("tmp_area_fullprod_click_count");
    }

    private static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession sparkSession) {
        String sql =
                "SELECT " +
                    "area," +
                    "CASE " +
                    "WHEN area='华北' OR area='华东' THEN 'A级' " +
                    "WHEN area='华南' OR area='华中' THEN 'B级' " +
                    "WHEN area='西北' OR area='西南' THEN 'C级' " +
                    "ELSE 'D级' " +
                    "END area_level," +
                    "product_id," +
                    "click_count," +
                    "city_infos," +
                    "product_name," +
                    "product_status " +
                "FROM (" +
                    "SELECT " +
                        "area," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status," +
                        "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
                    "FROM tmp_area_fullprod_click_count" +
                ") t " +
                "WHERE rank<=3";

        Dataset<Row> dataFrame = sparkSession.sql(sql);
        return dataFrame.javaRDD();
    }

    private static void persistAreaTop3ProductRDD(Long taskId, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<>();

        for (Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(row.getLong(3));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areaTop3ProductDAO.insertBatch(areaTop3Products);

    }
}
