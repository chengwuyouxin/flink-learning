package com.lpq.connector.utils;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class KafkaUtil {
    public static KafkaProducer<Long,String> getProducer(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",
                Constant.KAFKA_BOOSTRAP_SERVERS);
        props.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.LongSerializer");
        props.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Long,String> producer = new KafkaProducer<Long, String>(props);
        return producer;
    }

    public static Properties getKafkaConsumerProperties(String consumerGroupId){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","192.168.29.149:9092");
        props.setProperty("zookeeper.connect","192.168.29.149:2181");
        props.setProperty("group.id",consumerGroupId);
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.LongDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Kafka分区自动发现的周期
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,"3000");

        return props;
    }
}
