package com.gmg.spark.stream;

/**
 * @author gmg
 * @title: UserKafkaProducer
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/7 14:20
 */
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UserKafkaProducer extends Thread
{
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    public UserKafkaProducer(String topic)
    {
        props.put("metadata.broker.list", "master2:6667");
        props.put("bootstrap.servers", "master2:6667");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
    }
    @Override
    public void run() {
        int messageNo = 1;
        while (true)
        {
            String messageStr = new String("Message_" + messageNo);
            System.out.println("Send:" + messageStr);
            producer.send(new ProducerRecord<Integer, String>(topic, messageStr));
            messageNo++;
            try {
                sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}