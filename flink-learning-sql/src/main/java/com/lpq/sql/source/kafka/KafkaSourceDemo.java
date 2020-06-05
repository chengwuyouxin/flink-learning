package com.lpq.sql.source.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Flink read stream data from kafka
 *
 * 1) 使用ProcessingTime
 * 2) 读取kafka json数据，利用fastjson转成Order
 * 3) 将DataStream转为Table
 * 4) 使用SQL api 统计消费总额
 */

public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(env,settings);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers","192.168.29.149:9092");
        props.setProperty("zookeeper.connect","192.168.29.149:2181");
        props.setProperty("group.id","KafkaSourceDemo");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer011<String> consumer011 =
                new FlinkKafkaConsumer011<>(
                        "order",
                        new SimpleStringSchema(),
                        props
                );
        consumer011.setStartFromGroupOffsets();

        DataStreamSource<String> kafkaDataStreamSource = env.addSource(consumer011);

        DataStream<Order> orders = kafkaDataStreamSource.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                return JSON.parseObject(value,Order.class);
            }
        });

        sTableEnv.createTemporaryView("orders",orders,"name,product,amount,price");

        Table result = sTableEnv.sqlQuery("select name,sum(amount*price) from orders group by name");

        //查看sql执行计划
        String plans = sTableEnv.explain(result);
        System.out.println(plans);

//        sTableEnv.toRetractStream(result,Result.class).print();
//
//        env.execute("Flink read stream data from kafka!");
    }

    public static class Order{
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

    public static class Result{
        private String name;
        private double sum;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public Result() {
        }

        @Override
        public String toString() {
            return "Result{" +
                    "name='" + name + '\'' +
                    ", sum=" + sum +
                    '}';
        }
    }
}
