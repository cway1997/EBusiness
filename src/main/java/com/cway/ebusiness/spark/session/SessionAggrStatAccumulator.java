package com.cway.ebusiness.spark.session;

import com.cway.ebusiness.constant.Constants;
import com.cway.ebusiness.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 17:36 2019/8/8
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

    private String sessionAggrStat = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    /**
     * 返回该累加器是否为零值。
     *
     * @return
     */
    @Override
    public boolean isZero() {
        return sessionAggrStat.equals(Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0");
    }

    /**
     * 创建此该累加器的新副本
     *
     * @return
     */
    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator cp = new SessionAggrStatAccumulator();
        cp.sessionAggrStat = this.sessionAggrStat;
        return cp;
    }

    /**
     * 重置这个累加器，它的值为零。即调用isZero()返回true
     */
    @Override
    public void reset() {
        sessionAggrStat = Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * 接收输入并累加
     *
     * @param v
     */
    @Override
    public void add(String v) {

        String oldValue = StringUtils.getFieldFromConcatString(sessionAggrStat, "\\|", v);
        if (oldValue != null && !oldValue.equals("")) {
            int newValue = Integer.valueOf(oldValue) + 1;
            sessionAggrStat = StringUtils.setFieldInConcatString(sessionAggrStat, "\\|", v, String.valueOf(newValue));
        }

    }

    /**
     * 将另一个相同类型的累加器合并到这个累加器中并更新它的状态
     *
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        /**
         * 1.取出other的所有key=value
         * 2.逐个累加
         */
        String[] fields = other.value().split("\\|");
        for (String concatField : fields) {
            // searchKeywords=|clickCategoryIds=1,2,3
            if (concatField.split("=").length == 2) {
                String fieldName = concatField.split("=")[0];
                String fieldValue = concatField.split("=")[1];
                String oldValue = StringUtils.getFieldFromConcatString(sessionAggrStat, "\\|", fieldName);
                if (oldValue != null && !oldValue.equals("")) {
                    int newValue = Integer.valueOf(oldValue) + Integer.valueOf(fieldValue);
                    sessionAggrStat = StringUtils.setFieldInConcatString(sessionAggrStat, "\\|", fieldName, String.valueOf(newValue));
                }
            }
        }

    }

    /**
     * AccumulatorV2对外访问的数据结果(定义此累加器的当前值)
     *
     * @return
     */
    @Override
    public String value() {
        return sessionAggrStat;
    }
}
