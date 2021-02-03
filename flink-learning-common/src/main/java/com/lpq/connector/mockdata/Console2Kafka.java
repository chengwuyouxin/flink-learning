package com.lpq.connector.mockdata;

import com.alibaba.fastjson.JSON;
import com.lpq.connector.dao.ConsoleRecord;
import com.lpq.connector.utils.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/6/10
 */
public class Console2Kafka {
    public static void main(String[] args) throws ParseException, IOException {
        KafkaProducer<Long,String> producer = KafkaUtil.getProducer();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String topic = "consolerecord";

        while(true){
            String str = br.readLine();
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
            producer.send(new ProducerRecord<Long,String>(topic, timestamp, JSON.toJSONString(order)));
            System.out.println(JSON.toJSONString(order) + " is send to kafka");
        }
    }

}
