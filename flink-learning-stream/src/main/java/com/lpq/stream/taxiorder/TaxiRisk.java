package com.lpq.stream.taxiorder;

import com.alibaba.fastjson.JSON;
import com.lpq.flinklearning.dao.TaxiOrder;

import com.lpq.flinklearning.kafka.KafkaAssignerWithPeriodicWatermarks;
import com.lpq.flinklearning.kafka.MyJsonDeserializationSchema;
import com.lpq.flinklearning.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class TaxiRisk {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<ObjectNode> consumer011 =
                new FlinkKafkaConsumer011<>(
                        "taxiorder",
                        new MyJsonDeserializationSchema(true),
                        KafkaUtil.getKafkaConsumerProperties("TaxiOrderConsumer")
                );


        consumer011.setStartFromGroupOffsets();
        //如果不设置，不会发送watermark
        consumer011.assignTimestampsAndWatermarks(new KafkaAssignerWithPeriodicWatermarks());

        DataStream<ObjectNode> kafkaStream = env.addSource(consumer011);
        DataStream<TaxiOrder> orders = kafkaStream.map(new MapFunction<ObjectNode, TaxiOrder>() {
            @Override
            public TaxiOrder map(ObjectNode value) throws Exception {
                String order = value.get("value").toString();
                return JSON.parseObject(order,TaxiOrder.class);
            }
        });


        orders.process(new MyProcessFunction()).print();

        env.execute();

    }

    private static class MyProcessFunction
            extends ProcessFunction<TaxiOrder, Tuple3<String,String,String>>{

        @Override
        public void processElement(TaxiOrder value, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hh:MM:ss.SSS");
            String recordProduceTime = sdf.format(new Date(value.getOrderTime()));
            String watermark = sdf.format(new Date(ctx.timerService().currentWatermark()));
            String processingTime = sdf.format(new Date(ctx.timerService().currentProcessingTime()));
            out.collect(Tuple3.of(recordProduceTime,watermark,processingTime));
        }
    }
}
