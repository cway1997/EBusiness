package com.cway.ebusiness.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.test.MockData;
import com.cway.ebusiness.util.ParamUtils;
import com.cway.ebusiness.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @Author: Cway
 * @Description: 用户访问session分析spark作业
 * @Date: Create in 17:20 2019/8/7
 * <p>
 * 指定条件：
 * 1.时间范围
 * 2.性别
 * 3.年龄范围
 * 4.职业：多选
 * 5.城市：多选
 * 6.搜索词
 * 7.点击品类
 */
public class UserVisitSessionAnalyze {
    public static void main(String[] args) {
        // 构建上下文
//        SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
//        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate();
        SparkSession sparkSession = SparkSession.builder()
                .appName(Constants.SPARK_APP_NAME_SESSION)
                .master("local[*]")
//        .config("xxx","xx")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
//        SQLContext sqlContext = getSQLContext(sparkSession);

        // 生成模拟测试数据
        mockData(sc, sparkSession);
        // user_visit_action,指定日期范围
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDao.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sparkSession, taskParam);

        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sparkSession, actionRDD);



        sc.close();
    }

    /**
     * 获取指定日期内的用户访问行为
     *
     * @param sparkSession
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession sparkSession, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * " +
                "from user_visit_action " +
                "where date>='" + startDate + "'" +
                "and date<='" + endDate + "'";
        Dataset<Row> actionDS = sparkSession.sql(sql);
        return actionDS.javaRDD();
    }

    /**
     * session粒度聚合
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SparkSession sparkSession, JavaRDD<Row> actionRDD) {
        // Row -> <session,Row>
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            public static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });

        // 分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        // <userId, partAggrInfo(sessionId,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String sessionId = tuple2._1;
                Iterator<Row> iterator = tuple2._2.iterator();

                StringBuffer searchKeywordBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                Long userId = null;

                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    if (userId == null) {
                        userId = row.getLong(1);
                    }
                    String seachKeyword = row.getString(5);
                    Long clickCategory = row.getLong(6);
                    // 并非所有行为都有这两个字段

                    if (StringUtils.isNotEmpty(seachKeyword)) {
                        if (!searchKeywordBuffer.toString().contains(seachKeyword)) {
                            searchKeywordBuffer.append(seachKeyword + ",");
                        }
                    }

                    if (clickCategory != null) {
                        if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategory))) {
                            clickCategoryIdsBuffer.append(clickCategory + ",");
                        }
                    }


                }

                String searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                // key=value|key=value
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                return new Tuple2<Long, String>(userId, partAggrInfo);
            }
        });

        // 查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|" +
                        Constants.FIELD_AGE + "=" + age + "|" +
                        Constants.FIELD_PROFESSIONAL + "= " + professional + "|" +
                        Constants.FIELD_CITY + "=" + city + "|" +
                        Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<>(sessionId, fullAggrInfo);
            }
        });

        return sessionId2FullAggrInfoRDD;
    }

    /**
     * 获取SQLContext
     * 2.x之前
     * 如果是本地环境测试->SQLContext
     * 生产环境运行->HiveContext
     * 2.x之后统一使用SparkSession
     *
     * @param sparkSession
     * @return
     */
/*    private static SQLContext getSQLContext(SparkSession sparkSession) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sparkSession);
        } else {
            return new HiveContext(sparkSession);
        }
    }*/

    /**
     * 生成模拟数据
     *
     * @param sc
     * @param sparkSession
     */
    private static void mockData(JavaSparkContext sc, SparkSession sparkSession) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sparkSession);
        }
    }
}
