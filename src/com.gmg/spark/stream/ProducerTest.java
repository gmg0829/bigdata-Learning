package com.gmg.spark.stream;

/**
 * @author gmg
 * @title: Test
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/7 14:25
 */
public class ProducerTest {
    public static void main(String[] args) {
        UserKafkaProducer producerThread = new UserKafkaProducer("topic1");
        producerThread.start();
    }
}
