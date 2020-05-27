package com.lpq.flinklearning.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @author liupengqiang
 * @date 2020/5/25
 */
public class DataEmit2 {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.29.149:9092");
        prop.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.LongSerializer");
        prop.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Long,String> producer =
                new KafkaProducer<Long, String>(prop);

        String topic = "order";
        String[] users = {"xmm","lpq","ljx"};
        String[] products = {"food","water","coffee","car"};

        Random random = new Random();

        while(true){
            try{
                Order order = new Order(
                        users[random.nextInt(3)],
                        products[random.nextInt(4)],
                        random.nextInt(100),
                        random.nextDouble()
                );

                long timestamp = System.currentTimeMillis();
                //同步发送数据
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<Long,String>(topic,timestamp,
                        JSON.toJSONString(order))).get();

                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
                System.out.println(String.format("Message %s was sent to pattition: %d,offset:%d,Message Content: %s",
                        sdf.format(new Date(timestamp)),recordMetadata.partition(),recordMetadata.offset(),JSON.toJSONString(order)));
            }catch(Exception ex){
                ex.printStackTrace();
            }
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
