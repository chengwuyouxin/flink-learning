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
 * 货币类型（String）
 * 汇率（Integer）
 */
public class EmitRate2Kafka {
    public static void main(String[] args) {
        Producer<Long,String> producer = KafkaUtil.getProducer();
        String[] currencies = {"BEF","CNY","DEM","EUR","HKD","USD","ITL"};
        Random random = new Random();
        String topic = "rate";
        while(true){
            long timestamp = System.currentTimeMillis();
            String rate_record = String.format("%d,%s,%d",timestamp,
                    currencies[random.nextInt(currencies.length)],random.nextInt(20));

            try {
                RecordMetadata metadata = producer.send(new ProducerRecord<Long,String>(
                        topic,timestamp,rate_record
                )).get();
                System.out.println(String.format("Message %s is send to topic %s topic %d offset %d ",
                        rate_record,metadata.topic(),metadata.partition(),metadata.offset()));
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
