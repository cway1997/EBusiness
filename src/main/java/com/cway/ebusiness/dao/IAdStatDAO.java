package com.cway.ebusiness.dao;

import com.cway.ebusiness.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 *
 * @author Administrator
 */
public interface IAdStatDAO {

    void updateBatch(List<AdStat> adStats);

}
