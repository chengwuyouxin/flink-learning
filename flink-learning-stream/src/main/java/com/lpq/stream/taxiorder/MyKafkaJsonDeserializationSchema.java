package com.lpq.stream.taxiorder;

import com.lpq.connector.dao.TaxiOrder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class MyKafkaJsonDeserializationSchema implements KafkaDeserializationSchema<TaxiOrder> {

//    public MyKafkaJsonDeserializationSchema(boolean include){
//
//    }

    @Override
    public boolean isEndOfStream(TaxiOrder nextElement) {
        return false;
    }

    @Override
    public TaxiOrder deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TaxiOrder taxiOrder = null;
        if(record.value() != null){
            taxiOrder = mapper.readValue(record.value(), TaxiOrder.class);
        }
        return taxiOrder;
    }

    @Override
    public TypeInformation<TaxiOrder> getProducedType() {
        return null;
    }
}
