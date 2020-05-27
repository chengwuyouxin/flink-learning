package com.lpq.stream.transformation;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liupengqiang
 * @date 2020/5/17
 * split 输入DataStream 输出 SplitStream
 * 将指定的DataStream拆分成两个类型和输入流相同的输出流
 * union 输入DataStream 输出DataStream
 * 合并两条或多条类型相同的输入流
 */
public class SplitAndUnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> tempReadings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        SplitStream<SensorReading> splitted = tempReadings
                .split(new OutputSelector<SensorReading>() {
                    @Override
                    public Iterable<String> select(SensorReading sensorReading) {
                        List<String> output = new ArrayList<>();
                        if(sensorReading.temperature > 50) {
                            output.add("High");
                        } else {
                            output.add("Low");
                        }
                        return output;
                    }
                });

        DataStream<SensorReading> high = splitted.select("High");
        DataStream<SensorReading> low = splitted.select("Low");

//        high.print();
//        low.print();

        DataStream<SensorReading> all = high.union(low);

        all.print();

        env.execute("Flink Split Demp!");
    }
}
