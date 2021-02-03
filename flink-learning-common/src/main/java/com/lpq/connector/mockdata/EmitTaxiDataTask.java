package com.lpq.connector.mockdata;

import com.alibaba.fastjson.JSON;
import com.lpq.connector.dao.TaxiOrder;
import com.lpq.connector.utils.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class EmitTaxiDataTask implements Runnable{

    String[] sexes = {"M","F"};
    int[] cancels = {0,1};
    Random random = new Random();
    String topic = "taxiorder";

    @Override
    public void run() {

        KafkaProducer<Long,String> producer = KafkaUtil.getProducer();

        while(true){
            TaxiOrder order = new TaxiOrder(
                    System.currentTimeMillis(),
                    random.nextInt(100),
                    sexes[random.nextInt(2)],
                    random.nextInt(100),
                    sexes[random.nextInt(2)],
                    cancels[random.nextInt(2)]
            );
            long timestamp = System.currentTimeMillis();
            producer.send(new ProducerRecord<Long,String>(topic,
                    timestamp, JSON.toJSONString(order)));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
