package com.lpq.sql.dataemit;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class DataEmit {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.29.149:9092");
        prop.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        prop.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer,String> producer =
                new KafkaProducer<Integer, String>(prop);

        String topic = "order";
        String[] users = {"xmm","lpq","ljx"};
        String[] products = {"food","water","coffee","car"};

        Random random = new Random();
        int msgNo = 0;

        while(true){
            try{
                Order order = new Order(
                        users[random.nextInt(3)],
                        products[random.nextInt(4)],
                        random.nextInt(100),
                        random.nextDouble()
                );
                //同步发送数据
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<Integer,String>(topic,msgNo,
                        JSON.toJSONString(order))).get();

                System.out.println(String.format("Message %d was sent to pattition: %d,offset:%d,Message Content: %s",
                        msgNo,recordMetadata.partition(),recordMetadata.offset(),JSON.toJSONString(order)));
            }catch(Exception ex){
                ex.printStackTrace();
            }
            msgNo++;
            try{
                Thread.sleep(1000);
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }

    static class Order{
        private String name;
        private String product;
        private int amount;
        private double price;

        public Order(String name, String product, int amount, double price) {
            this.name = name;
            this.product = product;
            this.amount = amount;
            this.price = price;
        }

        public String getName() {
            return name;
        }

        public String getProduct() {
            return product;
        }

        public int getAmount() {
            return amount;
        }

        public double getPrice() {
            return price;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public void setAmount(int amount) {
            this.amount = amount;
        }

        public void setPrice(double price) {
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
