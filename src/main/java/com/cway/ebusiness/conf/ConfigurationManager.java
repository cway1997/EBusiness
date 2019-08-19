package com.cway.ebusiness.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: Cway
 * @Description: 配置管理组件
 * @Date: Create in 15:03 2019/8/7
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();

    /**
     *
     */
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
