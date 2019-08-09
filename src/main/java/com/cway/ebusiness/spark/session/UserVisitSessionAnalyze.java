package com.cway.ebusiness.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.ISessionAggrStatDAO;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.SessionAggrStat;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.test.MockData;
import com.cway.ebusiness.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.Date;
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

        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        sc.sc().register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");
        JavaPairRDD<String, String> filteredSessionId2AggrRDD = filterSession(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        filteredSessionId2AggrRDD.count();
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),task.getTaskid());
        // 计算各个范围的session占比


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

            // session的起始和结束时间
            Date startTime = null;
            Date endTime = null;
            // session的访问步长
            int stepLength = 0;

            // 遍历所有session的访问行为
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

                // 计算session开始和结束时间
                Date actionTime = DateUtils.parseTime(row.getString(4));
                if (startTime == null) {
                    startTime = actionTime;
                }

                if (endTime == null) {
                    endTime = actionTime;
                }

                if (actionTime.before(startTime)) {
                    startTime = actionTime;
                }

                if (actionTime.after(endTime)) {
                    endTime = actionTime;
                }

                // 计算session访问步长
                stepLength++;

            }

            String searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString());
            String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

            long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

            // key=value|key=value
            String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                    Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
                    Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                    Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                    Constants.FIELD_STEP_LENGTH + "=" + stepLength;

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

    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessionId2AggrRDD, final JSONObject taskParam, final AccumulatorV2 accumulatorV2) {

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
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionId2AggrRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                String aggrInfo = tuple2._2;

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

                // 通过筛选保留的session，对其访问时长和访问步长进行统计，累加
                // session总数
                accumulatorV2.add(Constants.SESSION_COUNT);
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateStepLength(stepLength);
                calculateVisitLength(visitLength);

                return true;
            }

            private void calculateVisitLength(long visitLength) {
                if (visitLength >= 1 && visitLength <= 3) {
                    accumulatorV2.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitLength >= 4 && visitLength <= 6) {
                    accumulatorV2.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitLength >= 7 && visitLength <= 9) {
                    accumulatorV2.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitLength >= 10 && visitLength <= 30) {
                    accumulatorV2.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitLength > 30 && visitLength <= 60) {
                    accumulatorV2.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitLength > 60 && visitLength <= 180) {
                    accumulatorV2.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitLength > 180 && visitLength <= 600) {
                    accumulatorV2.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitLength > 600 && visitLength <= 1800) {
                    accumulatorV2.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitLength > 1800) {
                    accumulatorV2.add(Constants.TIME_PERIOD_30m);
                }
            }

            private void calculateStepLength(long stepLength) {
                if (stepLength >= 1 && stepLength <= 3) {
                    accumulatorV2.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLength >= 4 && stepLength <= 6) {
                    accumulatorV2.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLength >= 7 && stepLength <= 9) {
                    accumulatorV2.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLength >= 10 && stepLength <= 30) {
                    accumulatorV2.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLength > 30 && stepLength <= 60) {
                    accumulatorV2.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLength > 60) {
                    accumulatorV2.add(Constants.STEP_PERIOD_60);
                }
            }
        });


        return filteredSessionId2AggrInfoRDD;
    }

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

    private static void calculateAndPersistAggrStat(String value, long taskId) {
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble((double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble((double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double)step_length_60 / (double)session_count, 2);

        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);

        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }
}
