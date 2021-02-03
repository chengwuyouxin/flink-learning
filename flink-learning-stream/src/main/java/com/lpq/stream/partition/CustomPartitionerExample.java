package com.lpq.stream.partition;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2020/6/1
 *
 * 通过实现Partitioner接口实现自定义分区器
 */
public class CustomPartitionerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        DataStream<SensorReading> partitioned = input.partitionCustom(new MyPartitioner(), new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.id;
            }
        });

        partitioned.print();

        env.execute();

    }

    private static class MyPartitioner implements Partitioner<String>{

        @Override
        public int partition(String key, int numPartitions) {
            int id = Integer.valueOf(key.split("_")[1]);
            if(id % 2 == 0){
                return 0;
            }else{
                return 1;
            }
        }
    }
}
