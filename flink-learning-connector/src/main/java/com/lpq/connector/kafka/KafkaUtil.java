package com.lpq.connector.kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * @author liupengqiang
 * @date 2020/5/25
 */
public class KafkaUtil {
    public static Properties getKafkaConsumerProperties(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","192.168.29.149:9092");
        props.setProperty("zookeeper.connect","192.168.29.149:2181");
        props.setProperty("group.id","FlinkKafkaConsumerEventTimeDemo");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.LongDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Kafka分区自动发现的周期
        props.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,"3000");

        return props;
    }
}
