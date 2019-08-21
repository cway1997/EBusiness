package com.cway.ebusiness.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * @Author: Cway
 * @Description: 组内拼接去重函数
 * @Date: Create in 23:07 2019/8/21
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    /**
     * 输入数据字段和类型
     */
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));

    /**
     * 缓冲数据字段与类型
     */
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));

    /**
     * 返回类型
     */
    private DataType dataType = DataTypes.StringType;

    /**
     * 是否是确定性的
     */
    private boolean deterministic = true;


    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化
     * 内部指定初始值
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /**
     * 更新
     *
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 缓冲中的已经拼接过得城市信息
        String bufferCityInfo = buffer.getString(0);
        // 传入的某个城市信息
        String cityInfo = input.getString(0);

        // 实现去重逻辑
        if (!bufferCityInfo.contains(cityInfo)){
            if ("".equals(bufferCityInfo)){
                bufferCityInfo += cityInfo;
            } else {
              bufferCityInfo += "," + cityInfo;
            }

            buffer.update(0, bufferCityInfo);
        }
    }

    /**
     * 合并
     * 分布式的串合并
     *
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for (String cityInfo : bufferCityInfo2.split(",")) {
            if (!bufferCityInfo1.contains(cityInfo)){
                if ("".equals(bufferCityInfo1)){
                    bufferCityInfo1 += cityInfo;
                }else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }

        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
