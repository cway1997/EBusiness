package com.cway.ebusiness.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 11:17 2019/8/22
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(num);
        return randNum + "_" +val;
    }
}
