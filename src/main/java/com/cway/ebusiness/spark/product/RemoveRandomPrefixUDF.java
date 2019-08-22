package com.cway.ebusiness.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 11:26 2019/8/22
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {

    @Override
    public String call(String val) throws Exception {
        return val.split("_")[1];
    }
}
