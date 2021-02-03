package com.lpq.connector.mockdata;

import com.alibaba.fastjson.JSON;
import com.lpq.connector.dao.ConsoleRecord;
import com.lpq.connector.utils.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/6/10
 */
public class File2Kafka {
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        FileInputStream fileInputStream = new FileInputStream("e:/console.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,"utf-8"));
        KafkaProducer<Long,String> producer = KafkaUtil.getProducer();
        String str = null;
        String topic = "consolerecord";
        while((str = bufferedReader.readLine()) != null){
            System.out.println(str);
            String[] sa = str.split(",");

            SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat sdfTime = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
            String date = sdfDate.format(new Date());
            String time = date + " " + sa[0];

            Long timestamp = sdfTime.parse(time).getTime();

            ConsoleRecord order = new ConsoleRecord(
                    timestamp,
                    Integer.valueOf(sa[1]),
                    sa[2]
            );

            try{
                RecordMetadata metadata=producer.send(new ProducerRecord<Long, String>(
                        topic,timestamp,JSON.toJSONString(order))).get();
                System.out.println(String.format("Message %s was sent to topic:%s,pattition: %d,offset:%d",
                        JSON.toJSONString(order),metadata.topic(),metadata.partition(),metadata.offset()));
            }catch(Exception e){
                e.printStackTrace();
            }

            Thread.sleep(1000);
        }
        
    }
}
