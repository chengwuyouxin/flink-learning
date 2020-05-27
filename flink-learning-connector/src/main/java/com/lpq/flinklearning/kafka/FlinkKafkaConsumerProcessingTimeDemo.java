package com.lpq.flinklearning.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

/**
 * @author liupengqiang
 * @date 2020/5/25
 */
public class FlinkKafkaConsumerProcessingTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(
                "order",  //主题
                new SimpleStringSchema(),
                KafkaUtil.getKafkaConsumerProperties()  //属性
        );

        //设置kafka从消费者群组记录的offset处开始读取
        consumer011.setStartFromEarliest();

        DataStream<String> kafkaSourceStream = env.addSource(consumer011);


        kafkaSourceStream.print();

        env.execute("Read Data From Kafka Source!");
    }
}
