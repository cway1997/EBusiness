package com.cway.ebusiness.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @Author: Cway
 * @Description:
 * @Date: Create in 14:29 2019/8/12
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public CategorySortKey(Long clickCount, Long orderCount, Long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (clickCount < that.getClickCount()) {
            return true;
        } else if (clickCount.equals(that.getClickCount()) &&
                orderCount < that.getOrderCount()) {
            return true;
        } else if (clickCount.equals(that.getClickCount()) &&
                orderCount.equals(that.getOrderCount()) &&
                payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(clickCount > that.getClickCount()) {
            return true;
        } else if(clickCount.equals(that.getClickCount()) &&
                orderCount > that.getOrderCount()) {
            return true;
        } else if(clickCount.equals(that.getClickCount()) &&
                orderCount.equals(that.getOrderCount()) &&
                payCount > that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if($less(that)) {
            return true;
        } else if(clickCount.equals(that.getClickCount()) &&
                orderCount.equals(that.getOrderCount()) &&
                payCount.equals(that.getPayCount())) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if($greater(that)) {
            return true;
        } else if(clickCount.equals(that.getClickCount()) &&
                orderCount.equals(that.getOrderCount()) &&
                payCount.equals(that.getPayCount())) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        if (clickCount - that.getClickCount() != 0) {
            return (int) (clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int) (orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int) (payCount - that.getPayCount());
        }
        return 0;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }
}
