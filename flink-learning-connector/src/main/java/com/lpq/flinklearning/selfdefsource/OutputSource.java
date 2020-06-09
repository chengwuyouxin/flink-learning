package com.lpq.flinklearning.selfdefsource;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2020/6/8
 */
public class OutputSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        DataStream<Long> source = env.addSource(new SourceFunctionDemo());

        source.print();

        env.execute("Long source!");
    }
}
