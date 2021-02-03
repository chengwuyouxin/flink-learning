package com.lpq.connector.mockdata;

import com.lpq.connector.utils.KafkaUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * @author liupengqiang
 * @date 2020/6/11
 *
 * 时间戳（Long）
 *
 * 商品（String）
 *
 * 货币类型（String）
 *
 * 价格（Integer）
 */
public class EimtOrder2Kafka {
    public static void main(String[] args) {
        Producer<Long,String> producer = KafkaUtil.getProducer();
        String[] currencies = {"BEF","CNY","DEM","EUR","HKD","USD","ITL"};
        String[] products = {"Phone","Computer","keyboard","mouse","LED","CUP"};
        Random random = new Random();
        String topic = "order";
        while(true){
            long timestamp = System.currentTimeMillis();
            String record = String.format("%d,%s,%s,%d",timestamp,
                    products[random.nextInt(products.length)],
                    currencies[random.nextInt(currencies.length)],
                    random.nextInt(200));

            try {
                RecordMetadata metadata = producer.send(new ProducerRecord<Long,String>(
                     topic,
                     timestamp,
                     record
                )).get();
                System.out.println(String.format("Message %s is send to topic %s topic %d offset %d ",
                        record,metadata.topic(),metadata.partition(),metadata.offset()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }
}
