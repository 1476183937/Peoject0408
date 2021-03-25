package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountInterceptor implements ProducerInterceptor<String,String> {

    int success;
    int error;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if (exception == null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("successed : "+success);
        System.out.println("error : "+error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
