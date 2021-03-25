package com.atguigu.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Properties properties = new Properties();
        //设置连接到的kafka
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //设置key、value的序列化
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        properties.put("acks", "all");
        //重试次数
        properties.put("retries", 1);
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<>("first", "hnust"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null){
                        System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset());
                    }else{
                        System.out.println(e);
                    }
                }
            });

        }

        producer.close();


    }
}
