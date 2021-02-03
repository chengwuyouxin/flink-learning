package com.lpq.stream.window.trigger;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.text.SimpleDateFormat;

/**
 * @author liupengqiang
 * @date 2021/1/28
 */
public class ContinuousEventTimeTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<SensorReading> input=env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        input.keyBy(r->r.id)
                .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(-8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        SimpleDateFormat sdf=new SimpleDateFormat("HH:mm:ss");
                        return new SensorReading(
                                value1.id,
                                value1.timestamp,
                                (value1.temperature+value2.temperature)/2

                        );
                    }
                })
                .print();
        env.execute();
    }
}
