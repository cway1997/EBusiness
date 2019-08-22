package com.cway.ebusiness.spark.ad;

import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.dao.*;
import com.cway.ebusiness.dao.factory.DAOFactory;
import com.cway.ebusiness.domain.*;
import com.cway.ebusiness.util.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.HiveUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 15:46 2019/8/22
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");
//                .set("spark.streaming.blockInterval", "50")
//                .set("spark.streaming.receiver.writeAheadLog.enable", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("hdfs://192.168.201.101:9090/streaming_checkpoint");

        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put(Constants.BOOTSTRAP_SERVERS, ConfigurationManager.getProperty(Constants.BOOTSTRAP_SERVERS));
//        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", ConfigurationManager.getProperty(Constants.GROUP_ID));
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);

        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Collection<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }
        try {
            // timestamp province city userId adId
//        KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
            JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams));

            // 原始用户点击行为根据进行黑名单进行过滤
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream = getAdFilteredAdRealTimeLogDStream(adRealTimeLogDStream);

            // 生成动态黑名单
            generateDynamicBlacklist(filteredAdRealTimeLogDStream);

            // 计算广告点击流量实时统计结果   (yyyyMMdd_province_city_adId, clickCount)
            JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

            // 实时统计每天每个省份top3热门广告
            calculateProvinceTop3Ad(adRealTimeStatDStream);

            // 实时统计每天每个广告在最近1小时内的滑动窗口内的点击趋势(每分钟的点击量)
            calculateAdClickCountByWindow(adRealTimeLogDStream);


            jssc.start();
            jssc.awaitTermination();
            jssc.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    private static JavaPairDStream<String, String> getAdFilteredAdRealTimeLogDStream(
            JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream) {
        return adRealTimeLogDStream.transformToPair(
                (Function<JavaRDD<ConsumerRecord<String, String>>, JavaPairRDD<String, String>>) rdd -> {
                    IAdBlacklistDAO blacklistDAO = DAOFactory.getAdBlacklistDAO();
                    List<AdBlacklist> adBlacklists = blacklistDAO.findAll();

                    List<Tuple2<Long, Boolean>> tuple2s = new ArrayList<>();

                    for (AdBlacklist adBlacklist : adBlacklists) {
                        tuple2s.add(new Tuple2<>(adBlacklist.getUserid(), true));
                    }

                    JavaSparkContext sc = new JavaSparkContext(rdd.context());
                    JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuple2s);

                    // 将rdd映射成<userId, str1|str2>
                    JavaPairRDD<Long, Tuple2<String, String>> mapRDD = rdd.mapToPair(
                            (PairFunction<ConsumerRecord<String, String>, Long, Tuple2<String, String>>) s -> {
                                String log = s.value();
                                String[] logSplited = log.split("_");

                                Long userId = Long.valueOf(logSplited[3]);

                                return new Tuple2(userId, new Tuple2<>(s.key(), s.value()));
                            });

                    // 左外连接
                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinRDD = mapRDD.leftOuterJoin(blacklistRDD);

                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinRDD.filter(
                            (Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>) tuple2 -> {
                                Optional<Boolean> optional = tuple2._2._2;
                                //
                                if (optional.isPresent() && optional.get()) {
                                    return false;
                                }
                                return true;
                            });

                    JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                            (PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>)
                                    tuple2 -> tuple2._2._1);

                    return resultRDD;
                });
    }

    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // <yyyyMMdd_userId_adId, 1L>
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
                (PairFunction<Tuple2<String, String>, String, Long>) tuple2 -> {
                    String log = tuple2._2;
                    String[] logSplited = log.split(" ");

                    String timestamp = logSplited[0];
                    Date date = new Date(Long.valueOf(timestamp));
                    String dateKey = DateUtils.formatDateKey(date);

                    long userId = Long.valueOf(logSplited[3]);
                    long adId = Long.valueOf(logSplited[4]);

                    String key = dateKey + "_" + userId + "_" + adId;

                    return new Tuple2<>(key, 1L);
                });

        // <yyyyMMdd_userId_adId, clickCount>
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                (Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

        dailyUserAdClickCountDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>) rdd ->
                rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
                    List<AdUserClickCount> list = new ArrayList<>();

                    while (iterator.hasNext()) {
                        Tuple2<String, Long> tuple2 = iterator.next();
                        String[] keySplit = tuple2._1.split("_");
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplit[0]));
                        Long userId = Long.valueOf(keySplit[1]);
                        Long adId = Long.valueOf(keySplit[2]);
                        Long clickCount = tuple2._2;

                        AdUserClickCount adUserClickCount = new AdUserClickCount();
                        adUserClickCount.setAdid(adId);
                        adUserClickCount.setClickCount(clickCount);
                        adUserClickCount.setDate(date);
                        adUserClickCount.setUserid(userId);

                        list.add(adUserClickCount);
                    }

                    IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                    adUserClickCountDAO.updateBatch(list);
                }));

        //
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(
                (Function<Tuple2<String, Long>, Boolean>) tuple2 -> {
                    String key = tuple2._1;
                    String[] keySplited = key.split("_");

                    String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                    Long userId = Long.valueOf(keySplited[1]);
                    Long adId = Long.valueOf(keySplited[2]);

                    // 查询指定日期、用户对广告的点击量
                    IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                    int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId, adId);

                    if (clickCount >= 100) {
                        return true;
                    }
                    return false;
                });

        // 黑名单去重
        JavaDStream<Long> blacklistUserIdDStream = blacklistDStream.map(
                (Function<Tuple2<String, Long>, Long>) tuple2 -> Long.valueOf(tuple2._1.split("_")[1]));

        JavaDStream<Long> distincyBlackUserIdDStream = blacklistUserIdDStream.transform(
                (Function<JavaRDD<Long>, JavaRDD<Long>>) rdd -> rdd.distinct());

        // 动态黑名单
        distincyBlackUserIdDStream.foreachRDD((VoidFunction<JavaRDD<Long>>) rdd ->
                rdd.foreachPartition((VoidFunction<Iterator<Long>>) iterator -> {
                    List<AdBlacklist> list = new ArrayList<>();

                    while (iterator.hasNext()) {
                        Long userId = iterator.next();

                        AdBlacklist adBlacklist = new AdBlacklist();
                        adBlacklist.setUserid(userId);

                        list.add(adBlacklist);
                    }

                    IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                    adBlacklistDAO.insertBatch(list);

                }));
    }

    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        JavaPairDStream<String, Long> mapDStream = filteredAdRealTimeLogDStream.mapToPair(
                (PairFunction<Tuple2<String, String>, String, Long>) tuple2 -> {
                    String log = tuple2._2;
                    String[] logSplited = log.split("_");

                    String timestamp = logSplited[0];
                    Date date = new Date(Long.valueOf(timestamp));
                    String dateKey = DateUtils.formatDateKey(date);
                    String province = logSplited[1];
                    String city = logSplited[2];
                    Long adId = Long.valueOf(logSplited[4]);

                    String key = dateKey + "_" + province + "_" + city + "_" + adId;

                    return new Tuple2<>(key, 1L);
                });

        JavaPairDStream<String, Long> aggregatedDStream = mapDStream.updateStateByKey(
                (Function2<List<Long>, Optional<Long>, Optional<Long>>) (list, optional) -> {
                    long clickCount = 0L;
                    if (optional.isPresent()) {
                        clickCount = optional.get();
                    }

                    for (Long value : list) {
                        clickCount += value;
                    }
                    return Optional.of(clickCount);
                });

        aggregatedDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>)
                rdd -> rdd.foreachPartition(
                        (VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
                            List<AdStat> list = new ArrayList<>();

                            while (iterator.hasNext()) {
                                Tuple2<String, Long> tuple2 = iterator.next();
                                String key = tuple2._1;
                                String[] keySplited = key.split("_");
                                String date = keySplited[0];
                                String province = keySplited[1];
                                String city = keySplited[2];
                                Long adId = Long.valueOf(keySplited[3]);

                                Long click_count = tuple2._2;

                                AdStat adStat = new AdStat();
                                adStat.setAdid(adId);
                                adStat.setCity(city);
                                adStat.setClickCount(click_count);
                                adStat.setDate(date);
                                adStat.setProvince(province);

                                list.add(adStat);
                            }
                            IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                            adStatDAO.updateBatch(list);
                        }));

        return aggregatedDStream;
    }

    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(
                (Function<JavaPairRDD<String, Long>, JavaRDD<Row>>) rdd -> {
                    JavaPairRDD<String, Long> mapRDD = rdd.mapToPair((PairFunction<Tuple2<String, Long>, String, Long>) tuple2 -> {
                        String[] keySplited = tuple2._1.split("_");
                        String date = keySplited[0];
                        String province = keySplited[1];
                        long adId = Long.valueOf(keySplited[3]);
                        Long clickCount = tuple2._2;

                        String key = date + "_" + province + "_" + adId;
                        return new Tuple2<>(key, clickCount);
                    });

                    JavaPairRDD<String, Long> dailyAdClickByProvinceRDD = mapRDD.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

                    JavaRDD<Row> rowsRDD = dailyAdClickByProvinceRDD.map((Function<Tuple2<String, Long>, Row>) tuple2 -> {
                        String[] keySplited = tuple2._1.split("_");
                        String dateKey = keySplited[0];
                        String province = keySplited[1];
                        long adId = Long.valueOf(keySplited[2]);
                        Long clickCount = tuple2._2;

                        String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));

                        return RowFactory.create(date, province, adId, clickCount);
                    });

                    StructType schema = DataTypes.createStructType(Arrays.asList(
                            DataTypes.createStructField("date", DataTypes.StringType, true),
                            DataTypes.createStructField("province", DataTypes.StringType, true),
                            DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                            DataTypes.createStructField("click_count", DataTypes.LongType, true)));

                    SparkSession sparkSession = SparkSession.builder().sparkContext(HiveUtils.withHiveExternalCatalog(rdd.context())).getOrCreate();
                    Dataset<Row> dailyAdClickByProvinceDF = sparkSession.createDataFrame(rowsRDD, schema);

                    dailyAdClickByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov");

                    Dataset<Row> provinceTop3DF = sparkSession.sql(
                            "SELECT " +
                                    "date," +
                                    "province," +
                                    "ad_id," +
                                    "click_count " +
                                    "FROM (" +
                                    "SELECT " +
                                    "date," +
                                    "province," +
                                    "ad_id," +
                                    "click_count," +
                                    "row_number() OVER(PARTITION BY province ORDER BY click_count DESC) rank " +
                                    "FROM tmp_daily_ad_click_count_by_prov" +
                                    ") t " +
                                    "WHERE rank>=3"
                    );

                    return provinceTop3DF.javaRDD();
                });

        rowsDStream.foreachRDD((VoidFunction<JavaRDD<Row>>) rdd ->
                rdd.foreachPartition((VoidFunction<Iterator<Row>>) iterator -> {
                    List<AdProvinceTop3> list = new ArrayList<>();

                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        String date = row.getString(0);
                        String province = row.getString(1);
                        long adId = row.getLong(2);
                        long clickCount = row.getLong(3);

                        AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                        adProvinceTop3.setAdid(adId);
                        adProvinceTop3.setClickCount(clickCount);
                        adProvinceTop3.setDate(date);
                        adProvinceTop3.setProvince(province);

                        list.add(adProvinceTop3);
                    }

                    IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                    adProvinceTop3DAO.updateBatch(list);
                })
        );
    }

    private static void calculateAdClickCountByWindow(JavaInputDStream<ConsumerRecord<String, String>> adRealTimeLogDStream) {
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(
                (PairFunction<ConsumerRecord<String, String>, String, Long>) s -> {
                    String[] logSplited = s.value().split("_");
                    String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                    long adId = Long.valueOf(logSplited[4]);
                    return new Tuple2<>(timeMinute + "_" + adId, 1L);
                });

        // <yyyyMMddhhmm_adId, 1L>
        JavaPairDStream<String, Long> aggrDStream = pairDStream.reduceByKeyAndWindow(
                (Function2<Long, Long, Long>) (v1, v2) -> v1 + v2,
                Durations.minutes(60),
                Durations.seconds(10));

        aggrDStream.foreachRDD((VoidFunction<JavaPairRDD<String, Long>>) rdd ->
                rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) iterator -> {
                    List<AdClickTrend> list = new ArrayList<>();

                    while (iterator.hasNext()) {
                        Tuple2<String, Long> tuple2 = iterator.next();
                        String[] keySplited = tuple2._1.split("_");

                        //yyyyMMddHHmm
                        String dateMinute = keySplited[0];
                        Long adId = Long.valueOf(keySplited[1]);
                        long clickCount = tuple2._2;

                        String date = DateUtils.formatDate(
                                DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                        String hour = dateMinute.substring(8, 10);
                        String minute = dateMinute.substring(10);

                        AdClickTrend adClickTrend = new AdClickTrend();
                        adClickTrend.setAdid(adId);
                        adClickTrend.setClickCount(clickCount);
                        adClickTrend.setDate(date);
                        adClickTrend.setHour(hour);
                        adClickTrend.setMinute(minute);

                        list.add(adClickTrend);
                    }

                    IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                    adClickTrendDAO.updateBatch(list);
                }));

    }
}
