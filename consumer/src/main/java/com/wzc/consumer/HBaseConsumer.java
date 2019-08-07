package com.wzc.consumer;

import com.wzc.Utils.PropertityUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Properties;

public class HBaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {

        //获取kafka配置信息
        Properties propertity = PropertityUtil.getPropertity();

        //创建kafka消费者并订阅主题
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(propertity);
        kafkaConsumer.subscribe(Collections.singletonList(propertity.getProperty("topics")));

        HBaseDao hBaseDao = new HBaseDao();

        //循环拉取数据打印
        try {
            while (true){
                ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());

                    //put
                    hBaseDao.put(consumerRecord.value());
                }
            }
        } finally {
            hBaseDao.close();
        }

    }
}
