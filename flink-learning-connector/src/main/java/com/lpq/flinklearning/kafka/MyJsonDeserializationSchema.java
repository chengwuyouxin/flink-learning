package com.lpq.flinklearning.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * @author liupengqiang
 * @date 2020/5/25
 *
 * 实现KafkaDeserializationSchema接口，解析kafka中的json数据
 */
public class MyJsonDeserializationSchema implements KafkaDeserializationSchema<ObjectNode> {

    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public MyJsonDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();

        if (record.key() != null) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.put(record.key(),0,record.key().length);
            buffer.flip();
            node.put("key", String.valueOf(buffer.getLong()));
        }
        if (record.value() != null) {
            node.set("value",mapper.readValue(record.value(), JsonNode.class));
        }
        if (includeMetadata) {
            node.putObject("metadata")
                    .put("offset", record.offset())
                    .put("topic", record.topic())
                    .put("partition", record.partition());
        }
        return node;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }
}