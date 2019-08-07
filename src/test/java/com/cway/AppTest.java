package com.cway;

import static org.junit.Assert.assertTrue;

import com.cway.ebusiness.conf.ConfigurationManager;
import com.cway.ebusiness.jdbc.JDBCHelper;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }

    @Test
    public void configurationManagerTest() {
        String driver = ConfigurationManager.getProperty("jdbc.driver");
        System.out.println(driver);
    }

    @Test
    public void JDBCHelperTest() {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        // update test
//        int rows = jdbcHelper.executeUpdate(
//                "insert into test_user(name,age) values(?,?)",
//                new Object[]{"王二", 22});
//        System.out.println("Affected rows: " + rows);

        // query test
//        Map<String, Object> testUser = new HashMap<>();
//        jdbcHelper.executeQuery(
//                "select name,age from test_user where name = ?",
//                new Object[]{"cway"},
//                new JDBCHelper.QueryCallback() {
//                    @Override
//                    public void process(ResultSet resultSet) throws Exception {
//                        if (resultSet.next()) {
//                            // 匿名内部类的使用，有一个很重要的知识点
//                            // 如果要访问外部类中的一些成员，比如方法内的局部变量
//                            // 那么，必须将局部变量，声明为final类型，才可以访问
//                            // 否则是访问不了的
//                            String name = resultSet.getString(1);
//                            int age = resultSet.getInt(2);
//                            testUser.put("name", name);
//                            testUser.put("age", age);
//                        }
//                    }
//                });
//        System.out.println("[name:" + testUser.get("name") + ", age:" + testUser.get("age") + "]");

        // batch test
        String sql = "insert into test_user(name,age) values(?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"麻子", 30});
        paramsList.add(new Object[]{"王五", 35});

        jdbcHelper.executeBatch(sql, paramsList);
    }
}
