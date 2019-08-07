package com.cway.ebusiness.dao.impl;

import com.cway.ebusiness.dao.IPageSplitConvertRateDAO;
import com.cway.ebusiness.domain.PageSplitConvertRate;
import com.cway.ebusiness.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 *
 * @author Administrator
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
