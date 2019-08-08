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
import com.cway.ebusiness.util.ValidUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
        SparkSession sparkSession = SparkSession.builder()
                .appName(Constants.SPARK_APP_NAME_SESSION)
                .master("local[*]")
//                .config("xxx","xx")
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

        // <sessionId, (sessionId, searchKeywords, clickCategoryIds, age, professional, city, sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sparkSession, actionRDD);

        System.out.println(sessionId2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple2 : sessionId2AggrInfoRDD.take(10)) {
            System.out.println(tuple2._2);
        }

        JavaPairRDD<String, String> filteredSessionId2AggrRDD = filterSession(sessionId2AggrInfoRDD, taskParam);
        System.out.println(filteredSessionId2AggrRDD.count());
        for (Tuple2<String, String> tuple2 : filteredSessionId2AggrRDD.take(10)) {
            System.out.println(tuple2._2);
        }

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

        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));

        // 分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        // <userId, partAggrInfo(sessionId,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair((PairFunction<Tuple2<String, Iterable<Row>>, Long, String>) tuple2 -> {
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
                Long clickCategory = (row.get(6) == null ? null : row.getLong(6));
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

            return new Tuple2<>(userId, partAggrInfo);
        });

        // 查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair((PairFunction<Row, Long, Row>) row -> new Tuple2<>(row.getLong(0), row));

        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair((PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>) tuple -> {
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
        });

        return sessionId2FullAggrInfoRDD;
    }

    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessionId2AggrRDD, final JSONObject taskParam) {

        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professional = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "") +
                (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "") +
                (professional != null ? Constants.PARAM_PROFESSIONALS + "=" + professional + "|" : "") +
                (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "") +
                (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "") +
                (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "") +
                (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionId2AggrRDD.filter((Function<Tuple2<String, String>, Boolean>) tuple -> {
            String aggrInfo = tuple._2;

            if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                return false;
            }

            if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                return false;
            }

            if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                return false;
            }

            if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                return false;
            }

            if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                return false;
            }

            if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                return false;
            }

            return true;
        });
        return filteredSessionId2AggrInfoRDD;
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
