package com.cway.ebusiness.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.*;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.*;
import com.cway.ebusiness.test.MockData;
import com.cway.ebusiness.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

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
                .master("local")
//                .config("xxx","xx")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        // 生成模拟测试数据
        mockData(sc, sparkSession);

        // user_visit_action,指定日期范围
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDao.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 原始RDD
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sparkSession, taskParam);

        // <sessionId, Row>
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        // <sessionId, (sessionId, searchKeywords, clickCategoryIds, age, professional, city, sex)>
        JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(sparkSession, actionRDD);

        // 自定义累加器
        SessionAggrStatAccumulator sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        // 注册累加器
        sc.sc().register(sessionAggrStatAccumulator);
        // 过滤，按条件筛选，累计session
        JavaPairRDD<String, String> filteredSessionId2AggrRDD = filterSession(
                sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        // 按比例随机抽取session
        randomExtractSession(sc, task.getTaskid(), filteredSessionId2AggrRDD, sessionId2ActionRDD);

        // 计算各个范围的session占比
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        // 得到热门商品种类
        getTop10Category(filteredSessionId2AggrRDD, sessionId2ActionRDD, task.getTaskid());

        sc.close();
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

    /**
     * 获取指定日期内的用户访问行为
     *
     * @param sparkSession sparkSession
     * @param taskParam    筛选维度(参数)
     * @return RowRDD
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
     * sessionId，Row映射
     *
     * @param actionRDD RowRDD
     * @return sessionId2RowRDD
     */
    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));
    }

    /**
     * session粒度聚合
     *
     * @param sparkSession sparkSession
     * @param actionRDD    RowRDD
     * @return (SessionId, aggrInfo)
     */
    private static JavaPairRDD<String, String> aggregateBySession(SparkSession sparkSession,
                                                                  JavaRDD<Row> actionRDD) {
        // Row -> <session,Row>
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));

        // 分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = session2ActionRDD.groupByKey();

        // <userId, partAggrInfo(sessionId,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair(
                (PairFunction<Tuple2<String, Iterable<Row>>, Long, String>) tuple2 -> {
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

                        // 并非所有行为都有这两个字段
                        String seachKeyword = row.getString(5);
                        Long clickCategory = (row.get(6) == null ? null : row.getLong(6));
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
                        String actionTimeStr = row.getString(4);
                        /**
                         * 本地模式不要开启多线程,DataUtils工具类有bug @_@!
                         * SimpleDateFormat类在多线程环境下中处理日期，极易出现日期转换错误的情况
                         */
                        Date actionTime = DateUtils.parseTime(actionTimeStr);
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
                            Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                            Constants.FIELD_START_TIME + "=" + startTime;

                    return new Tuple2<>(userId, partAggrInfo);
                });

        // 查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();

        // <userId, row>
        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(
                (PairFunction<Row, Long, Row>) row -> new Tuple2<>(row.getLong(0), row));

        // <userId, <aggrInfo, row>>
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);

        // <sessionId, fullAggrInfo>
        JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(
                (PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>) tuple -> {
                    String partAggrInfo = tuple._2._1;
                    Row userInfoRow = tuple._2._2;

                    String sessionId = StringUtils.getFieldFromConcatString(
                            partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

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

    /**
     * 过滤，按条件筛选session信息
     *
     * @param sessionId2AggrRDD session聚合信息
     * @param taskParam         筛选维度
     * @param accumulatorV2     累加器
     * @return
     */
    private static JavaPairRDD<String, String> filterSession(JavaPairRDD<String, String> sessionId2AggrRDD,
                                                             final JSONObject taskParam,
                                                             final AccumulatorV2 accumulatorV2) {
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
        JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionId2AggrRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
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

                        // session总数
                        accumulatorV2.add(Constants.SESSION_COUNT);

                        // 通过筛选保留的session，对其访问时长和访问步长进行统计，累加
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
     * 随机抽取session
     *
     * @param sessionId2AggrRDD
     */
    private static void randomExtractSession(JavaSparkContext sc,
                                             final Long taskId,
                                             JavaPairRDD<String, String> sessionId2AggrRDD,
                                             JavaPairRDD<String, Row> sessionId2ActionRDD) {
        // <"yyyy-MM-dd_HH", aggrInfo>
        JavaPairRDD<String, String> time2SessionRDD = sessionId2AggrRDD.mapToPair(
                (PairFunction<Tuple2<String, String>, String, String>) tuple2 -> {
                    String aggrInfo = tuple2._2;

                    String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                    String dateHour = DateUtils.getDateHour(startTime);

                    return new Tuple2<>(dateHour, aggrInfo);
                });

        //<"yyyy-MM-dd_HH", aggrInfo> -> <"yyyy-MM-dd_HH", count>
        Map<String, Long> countMap = time2SessionRDD.countByKey();

        //<"yyyy-MM-dd_HH", count> -> <"yyyy-MM-dd",<"HH", count>>
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();
        for (Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            Map<String, Long> hourCount = dateHourCountMap.get(date);
            if (hourCount == null) {
                hourCount = new HashMap<>();
                dateHourCountMap.put(date, hourCount);
            }
            Long count = Long.valueOf(countEntry.getValue());
            hourCount.put(hour, count);
        }

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <"yyyy-MM-dd",<"HH", count>> -> <"yyyy-MM-dd", <"HH", (3, 5, 8, 22, ..)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 一天session总数
            long sessionCount = 0L;
            for (long count : hourCountMap.values()) {
                sessionCount += count;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                int extractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (extractNumber > count) {
                    extractNumber = (int) count;
                }

                List<Integer> extractList = hourExtractMap.get(hour);
                if (extractList == null) {
                    extractList = new ArrayList<>();
                    hourExtractMap.put(hour, extractList);
                }

                for (int i = 0; i < extractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractList.add(extractIndex);
                }
            }
        }


        /**
         *
         * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
                new HashMap<String, Map<String, IntList>>();


        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry :
                dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();

                for (int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }

        /**
         * 广播变量，很简单
         * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
         */


        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastutilDateHourExtractMap);

        /**
         * 遍历每天每小时session，根据随机索引进行抽取
         */
        // <dateHour, (session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2SessionsRDD = time2SessionRDD.groupByKey();
        JavaPairRDD<String, String> extractSessionIdsRDD = time2SessionsRDD.flatMapToPair(
                (PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>) tuple2 -> {
                    List<Tuple2<String, String>> extractSession = new ArrayList<>();

                    String dateHour = tuple2._1;
                    String date = dateHour.split("_")[0];
                    String hour = dateHour.split("_")[1];
                    Iterator<String> iterator = tuple2._2.iterator();

                    Map<String, Map<String, IntList>> dateHourExtractMapBD =
                            dateHourExtractMapBroadcast.value();
                    List<Integer> sessionIndex = dateHourExtractMapBD.get(date).get(hour);
                    ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                    int index = 0;
                    while (iterator.hasNext()) {
                        String sessionInfo = iterator.next();

                        if (sessionIndex.contains(index)) {
                            String sessionId = StringUtils.getFieldFromConcatString(
                                    sessionInfo, "\\|", Constants.FIELD_SESSION_ID);
                            String startTime = StringUtils.getFieldFromConcatString(
                                    sessionInfo, "\\|", Constants.FIELD_START_TIME);
                            String searchKeyWords = StringUtils.getFieldFromConcatString(
                                    sessionInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
                            String clickCategoryIds = StringUtils.getFieldFromConcatString(
                                    sessionInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);

                            SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                            sessionRandomExtract.setTaskid(taskId);
                            sessionRandomExtract.setSessionid(sessionId);
                            sessionRandomExtract.setStartTime(startTime);
                            sessionRandomExtract.setSearchKeywords(searchKeyWords);
                            sessionRandomExtract.setClickCategoryIds(clickCategoryIds);

                            sessionRandomExtractDAO.insert(sessionRandomExtract);
                            extractSession.add(new Tuple2<>(sessionId, sessionId));
                        }

                        index++;
                    }
                    return extractSession.iterator();
                });

        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                (JavaPairRDD<String, Tuple2<String, Row>>) extractSessionIdsRDD.join(sessionId2ActionRDD);
        extractSessionDetailRDD.foreach((VoidFunction<Tuple2<String, Tuple2<String, Row>>>) tuple2 -> {
            Row row = tuple2._2._2;

            SessionDetail sessionDetail = new SessionDetail();
            sessionDetail.setTaskid(taskId);
            sessionDetail.setUserid(row.getLong(1));
            sessionDetail.setSessionid(row.getString(2));
            sessionDetail.setPageid(row.getLong(3));
            sessionDetail.setActionTime(row.getString(4));
            sessionDetail.setSearchKeyword(row.getString(5));
            Long clickCategoryId = (row.get(6) == null) ? null : row.getLong(6);
            Long clickProductId = (row.get(7) == null) ? null : row.getLong(7);
            sessionDetail.setClickCategoryId(clickCategoryId);
            sessionDetail.setClickProductId(clickProductId);
            sessionDetail.setOrderCategoryIds(row.getString(8));
            sessionDetail.setOrderProductIds(row.getString(9));
            sessionDetail.setPayCategoryIds(row.getString(10));
            sessionDetail.setPayProductIds(row.getString(11));

            ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
            sessionDetailDAO.insert(sessionDetail);
        });

    }

    private static void calculateAndPersistAggrStat(String value, long taskId) {
        long session_count = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(
                StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

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

    private static void getTop10Category(JavaPairRDD<String, String> filteredSessionId2AggrRDD,
                                         JavaPairRDD<String, Row> sessionId2ActionRDD,
                                         long taskId) {
        JavaPairRDD<String, Row> sessionId2DetailRDD = filteredSessionId2AggrRDD.join(sessionId2ActionRDD).mapToPair(
                (PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>)
                        tuple2 -> new Tuple2<>(tuple2._1, tuple2._2._2));

        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailRDD.flatMapToPair(
                (PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple2 -> {
                    Row row = tuple2._2;

                    List<Tuple2<Long, Long>> list = new ArrayList<>();
                    Long clickCategoryId = (row.get(6) != null ? row.getLong(6) : null);
                    if (clickCategoryId != null) {
                        list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                    }
                    String orderCategoryIds = row.getString(8);
                    if (orderCategoryIds != null && !"".equals(orderCategoryIds)) {
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                        }
                    }

                    String payCategoryIds = row.getString(10);
                    if (payCategoryIds != null && !"".equals(payCategoryIds)) {
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");
                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                        }
                    }

                    return list.iterator();
                });

        categoryIdRDD = categoryIdRDD.distinct();

        JavaPairRDD<Long, Long> clickCategoryIdsRDD = getClickCategoryIdsRDD(sessionId2DetailRDD);

        JavaPairRDD<Long, Long> orderCategoryIdsRDD = getOrderCategoryIdsRDD(sessionId2DetailRDD);

        JavaPairRDD<Long, Long> payCategoryIdsRDD = getPayCategoryIdsRDD(sessionId2DetailRDD);

        JavaPairRDD<Long, String> category2CountRDD = getCategory2CountRDD(
                categoryIdRDD, clickCategoryIdsRDD, orderCategoryIdsRDD, payCategoryIdsRDD);

        JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = category2CountRDD.mapToPair((PairFunction<Tuple2<Long, String>, CategorySortKey, String>) tuple2 -> {
            String countInfo = tuple2._2;

            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);

            return new Tuple2<>(categorySortKey, countInfo);
        });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);

        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);

        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String countInfo = tuple2._2;

            Long categoryId = Long.valueOf(
                    StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            Long clickCount = Long.valueOf(
                    StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(
                    StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(
                    StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setCategoryid(categoryId);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);
            top10Category.setTaskid(taskId);

            top10CategoryDAO.insert(top10Category);
        }

    }

    private static JavaPairRDD<Long, Long> getClickCategoryIdsRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailRDD.filter(
                (Function<Tuple2<String, Row>, Boolean>) tuple2 -> {
                    Row row = tuple2._2;
                    return row.get(6) != null;
                });

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                (PairFunction<Tuple2<String, Row>, Long, Long>) tuple2 -> {
                    Row row = tuple2._2;
                    return new Tuple2<>(row.getLong(6), 1L);
                });

        JavaPairRDD<Long, Long> clickCategoryIds = clickCategoryIdRDD.reduceByKey(
                (Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

        return clickCategoryIds;
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryIdsRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailRDD.filter(
                (Function<Tuple2<String, Row>, Boolean>) tuple2 -> {
                    Row row = tuple2._2;
                    return row.getString(8) != null;
                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                (PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple2 -> {
                    Row row = tuple2._2;
                    String orderCategoryIds = row.getString(8);
                    List<Tuple2<Long, Long>> list = new ArrayList<>();
                    for (String orderCategoeyId : orderCategoryIds.split(",")) {
                        list.add(new Tuple2<>(Long.valueOf(orderCategoeyId), 1L));
                    }
                    return list.iterator();
                });

        JavaPairRDD<Long, Long> orderCategoryIds = orderCategoryIdRDD.reduceByKey(
                (Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

        return orderCategoryIds;
    }

    private static JavaPairRDD<Long, Long> getPayCategoryIdsRDD(JavaPairRDD<String, Row> sessionId2DetailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionId2DetailRDD.filter(
                (Function<Tuple2<String, Row>, Boolean>) tuple2 -> {
                    Row row = tuple2._2;
                    return row.getString(10) != null;
                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                (PairFlatMapFunction<Tuple2<String, Row>, Long, Long>) tuple2 -> {
                    Row row = tuple2._2;
                    String payCategoryIds = row.getString(10);
                    List<Tuple2<Long, Long>> list = new ArrayList<>();
                    for (String payCategoeyId : payCategoryIds.split(",")) {
                        list.add(new Tuple2<>(Long.valueOf(payCategoeyId), 1L));
                    }
                    return list.iterator();
                });

        JavaPairRDD<Long, Long> payCategoryIds = payCategoryIdRDD.reduceByKey(
                (Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

        return payCategoryIds;
    }

    private static JavaPairRDD<Long, String> getCategory2CountRDD(
            JavaPairRDD<Long, Long> categoryIdRDD, JavaPairRDD<Long, Long> clickCategoryIdsRDD,
            JavaPairRDD<Long, Long> orderCategoryIdsRDD, JavaPairRDD<Long, Long> payCategoryIdsRDD) {
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tempJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryIdsRDD);

        JavaPairRDD<Long, String> tempMapRDD = tempJoinRDD.mapToPair(
                (PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>) tuple2 -> {
                    long categoryId = tuple2._1;
                    Optional<Long> optional = tuple2._2._2;
                    long clickCount = 0L;
                    if (optional.isPresent()) {
                        clickCount = optional.get();
                    }

                    String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
                            Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                    return new Tuple2<>(categoryId, value);
                });

        tempMapRDD = tempMapRDD.leftOuterJoin(orderCategoryIdsRDD).mapToPair(
                (PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>) tuple2 -> {
                    Long categoryId = tuple2._1;
                    Optional<Long> optional = tuple2._2._2;
                    String value = tuple2._2._1;
                    long orderCount = 0L;

                    if (optional.isPresent()) {
                        orderCount = optional.get();
                    }
                    value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                    return new Tuple2<>(categoryId, value);
                });

        tempMapRDD = tempMapRDD.leftOuterJoin(payCategoryIdsRDD).mapToPair(
                (PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>) tuple2 -> {
                    long categoeyId = tuple2._1;
                    String value = tuple2._2._1;
                    Optional<Long> optional = tuple2._2._2;

                    long payCount = 0L;
                    if (optional.isPresent()) {
                        payCount = optional.get();
                    }

                    value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                    return new Tuple2<>(categoeyId, value);
                });

        return tempMapRDD;
    }

}
