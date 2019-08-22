package com.cway.ebusiness.jdbc;

import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class JDBCHelper {
    static {
        try {
            Class.forName(ConfigurationManager.getProperty(Constants.JDBC_DRIVER));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance = null;

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> datasource = new LinkedList<>();

    private JDBCHelper() {
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        for (int i = 0; i < datasourceSize; i++) {
            String url = null;
            String user = null;
            String password = null;

            if (local) {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            } else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 执行增删改SQL
     *
     * @param sql
     * @param params
     * @return 影响行数
     */
    public int executeUpdate(String sql, Object[] params) {
        Connection conn = null;
        PreparedStatement pstmt = null;

        int rtn = 0;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if(params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 执行查询SQL
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if(params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行结构一样的SQL
     *
     * @param sql
     * @param paramsList
     * @return
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            // 取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            if(paramsList != null && paramsList.size() > 0) {
                // addBatch
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }
            rtn = pstmt.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 内部接口：查询回调
     */
    public static interface QueryCallback {
        /**
         * 处理查询结果
         *
         * @param resultSet
         * @throws Exception
         */
        void process(ResultSet resultSet) throws Exception;
    }
}
