package com.cway.ebusiness.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 23:43 2019/8/21
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {

    @Override
    public String call(String json, String field) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            return jsonObject.getString(field);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
