package com.lpq.connector.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/5/25
 */
public class FlinkKafkaConsumerEventTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //开启checkpoint保存kafka消费的offset
        env.enableCheckpointing(5000);

        FlinkKafkaConsumer011<ObjectNode> consumer011 = new FlinkKafkaConsumer011<>(
                //主题
                "order",
                new MyJsonDeserializationSchema(true),
                //属性
                KafkaUtil.getKafkaConsumerProperties()
        );
        //设置kafka从消费者群组记录的offset处开始读取
        consumer011.setStartFromGroupOffsets();
        consumer011.assignTimestampsAndWatermarks(new KafkaAssignerWithPeriodicWatermarks());

        DataStream<ObjectNode> kafkaSourceStream =env.addSource(consumer011);

        DataStream<Tuple2<String,Double>> res = kafkaSourceStream.map(new MapFunction<ObjectNode, Order>() {
            @Override
            public Order map(ObjectNode jsonNodes) throws Exception {
                String value = jsonNodes.get("value").toString();
                Order order = JSON.parseObject(value,Order.class);
                return order;
            }
        })
        .keyBy(new KeySelector<Order, String>() {
            @Override
            public String getKey(Order order) throws Exception {
                return order.name;
            }
        })
        .timeWindow(Time.seconds(5))
        .aggregate(new MyAggregateFunction());


        res.print();

        env.execute("Read Data From Kafka Source!");
    }

    private static class MyProcessFunction
            extends ProcessFunction<Order, Tuple2<String,String>> {

        @Override
        public void processElement(Order value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hh:MM:ss.SSS");
            String watermark = sdf.format(new Date(ctx.timerService().currentWatermark()));
            String processingTime = sdf.format(new Date(ctx.timerService().currentProcessingTime()));
            out.collect(Tuple2.of(watermark,processingTime));
        }
    }

    private static class MyAggregateFunction implements AggregateFunction<Order, Tuple2<String, Double>, Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> createAccumulator() {
            return Tuple2.of("",0.0);
        }

        @Override
        public Tuple2<String, Double> add(Order order, Tuple2<String, Double> acc) {
            double sum = acc.f1 + order.amount * order.price;
            return Tuple2.of(order.name,sum);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> acc1, Tuple2<String, Double> acc2) {
            return Tuple2.of(acc1.f0,acc1.f1 + acc2.f1);
        }
    }

    private static class Order{
        private String name;
        private String product;
        private int amount;
        private double price;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public int getAmount() {
            return amount;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public Order() {
        }

        public Order(String name, String product, int amount, double price) {
            this.name = name;
            this.product = product;
            this.amount = amount;
            this.price = price;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "name='" + name + '\'' +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    ", price=" + price +
                    '}';
        }
    }
}
