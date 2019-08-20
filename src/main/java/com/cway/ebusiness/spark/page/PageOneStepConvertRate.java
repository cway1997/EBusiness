package com.cway.ebusiness.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.IPageSplitConvertRateDAO;
import com.cway.ebusiness.dao.ITaskDAO;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.PageSplitConvertRate;
import com.cway.ebusiness.domain.Task;
import com.cway.ebusiness.util.DateUtils;
import com.cway.ebusiness.util.NumberUtils;
import com.cway.ebusiness.util.ParamUtils;
import com.cway.ebusiness.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

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
        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        // 2.生成模拟数据
        SparkUtils.mockData(sc, sparkSession);

        // 3.查询任务，获取任务的参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 4.查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sparkSession, taskParam);

        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(
                (PairFunction<Row, String, Row>) row -> new Tuple2<>(row.getString(2), row));

        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();

        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionId2ActionsRDD, taskParam);

        Map<String, Long> pageSplitPVMap = pageSplitRDD.countByKey();

        long startPagePV = getStartPagePV(sessionId2ActionsRDD, taskParam);

        Map<String, Double> pageSplitConvertRateMap = computePageSplitConvertRate(taskParam, pageSplitPVMap, startPagePV);

        persistConvertRate(task.getTaskid(), pageSplitConvertRateMap);
    }

    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD,
            JSONObject taskParam) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionId2ActionsRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>) tuple2 -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();

            Iterator<Row> iterator = tuple2._2.iterator();

            String[] targetPages = targetPageFlowBroadcast.value().split(",");

            List<Row> rows = new ArrayList<>();
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }

            Collections.sort(rows, (o1, o2) -> {
                String actionTime1 = o1.getString(4);
                String actionTime2 = o2.getString(4);

                Date time1 = DateUtils.parseTime(actionTime1);
                Date time2 = DateUtils.parseTime(actionTime2);
                return (int) (time1.getTime() - time2.getTime());
            });

            Long lastPageId = null;
            for (Row row : rows) {
                long pageId = row.getLong(3);

                if (lastPageId == null) {
                    lastPageId = pageId;
                    continue;
                }

                String pageSplit = lastPageId + "_" + pageId;
                for (int i = 1; i < targetPages.length; i++) {
                    String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];

                    if (targetPageSplit.equals(pageSplit)) {
                        list.add(new Tuple2<>(pageSplit, 1));
                        break;
                    }
                }

                lastPageId = pageId;
            }

            return list.iterator();
        });
    }

    private static long getStartPagePV(JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD,
                                       JSONObject taskParam) {

        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPagePVRDD = sessionId2ActionsRDD.flatMap((FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>) tuple2 -> {
            List<Long> list = new ArrayList<>();

            Iterator<Row> iterator = tuple2._2.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                Long pageId = row.getLong(3);
                if (pageId.equals(startPageId)) {
                    list.add(pageId);
                }
            }
            return list.iterator();
        });

        return startPagePVRDD.count();
    }

    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam,
                                                                   Map<String, Long> pageSplitPVMap,
                                                                   long startPagePV) {
        Map<String, Double> map = new HashMap<>();

        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastPageSplitPV = 0L;

        for (int i = 1; i < targetPages.length; i++) {
            String splitPage = targetPages[i - 1] + "_" + targetPages[i];
            long splitPagePV = pageSplitPVMap.get(splitPage);

            double convertRate = 0.0;
            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) splitPagePV / (double) startPagePV, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) splitPagePV / (double) lastPageSplitPV, 2);
            }

            map.put(splitPage, convertRate);
            lastPageSplitPV = splitPagePV;
        }

        return map;
    }

    /**
     * 持久化转化率
     *
     * @param convertRateMap
     */
    private static void persistConvertRate(long taskid,
                                           Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");

        for (Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }
}
