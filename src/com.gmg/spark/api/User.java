package com.gmg.spark.api;

import java.io.Serializable;

/**
 * @author gmg
 * @title: User
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2020/1/21 15:39
 */
public class User implements Serializable {
    private String userId;

    private Integer amount;

    public User(String userId, Integer amount) {
        this.userId = userId;
        this.amount = amount;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    //getter setter....
    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
