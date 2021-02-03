package com.lpq.stream.window;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2021/1/28
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<SensorReading> input=env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        input.keyBy(r->r.id)
                .countWindow(10)
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return new SensorReading(
                                value1.id,
                                value1.timestamp,
                                (value1.temperature+value2.temperature)/2

                        );
                    }
                }).print();
        env.execute();
    }
}
