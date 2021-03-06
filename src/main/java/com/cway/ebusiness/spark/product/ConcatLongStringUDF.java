package com.cway.ebusiness.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 23:03 2019/8/21
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return String.valueOf(v1) + split +v2;
    }
}
