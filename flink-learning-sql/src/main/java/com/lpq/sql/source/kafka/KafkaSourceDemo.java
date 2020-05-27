package com.lpq.sql.source.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Flink read stream data from kafka
 *应用会一直读取直到进程停止
 */

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers","192.168.29.149:9092");
        props.setProperty("zookeeper.connect","192.168.29.149:2181");
        props.setProperty("group.id","metric-group");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> kafkaDataStreamSource = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "order",
                        new SimpleStringSchema(),  //序列化
                        props
                )
        ).setParallelism(1);

        kafkaDataStreamSource.print();

        env.execute("Flink read stream data from kafka!");
    }
}
