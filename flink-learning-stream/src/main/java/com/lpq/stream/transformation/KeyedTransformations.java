package com.lpq.stream.transformation;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @author liupengqiang
 * @date 2020/5/16
 *
 * KeyedStream上可以执行的操作：
 * rolling aggregation
 * reduce
 * fold
 * keyedProcessFunction  具体实例可参照process function包中的KeyedProcessFunctionDemo
 */
public class KeyedTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //reduce
        DataStream<Tuple2<String, List<String>>> inputStream =
                env.fromElements(
                        Tuple2.of("en", Arrays.asList("tea")),
                        Tuple2.of("fr", Arrays.asList("vin")),
                        Tuple2.of("en", Arrays.asList("cake"))
                );
        KeyedStream<Tuple2<String, List<String>>,String> keyedStream =
            inputStream.keyBy(new KeySelector<Tuple2<String, List<String>>, String>() {
                        @Override
                        public String getKey(Tuple2<String, List<String>> r) throws Exception {
                            return r.f0;
                        }
                    });
        DataStream<Tuple2<String, List<String>>> result =
                keyedStream
                .reduce(new ReduceFunction<Tuple2<String, List<String>>>() {
                    @Override
                    public Tuple2<String, List<String>> reduce(Tuple2<String, List<String>> t1,
                                           Tuple2<String, List<String>> t2) throws Exception {
                        return Tuple2.of(t1.f0, mergeList(t1.f1,t2.f1));
                    }
                });
        result.print();

        //fold
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        input.keyBy(new KeySelector<SensorReading, String>() {
                @Override
                public String getKey(SensorReading sensorReading) throws Exception {
                    return sensorReading.id;
                }
             })
            .fold(new Tuple2<String, Double>("", 0.0), new FoldFunction<SensorReading, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> fold(Tuple2<String, Double> acc, SensorReading value) throws Exception {
                    return Tuple2.of(value.id, acc.f1 + value.temperature);
                }
            }).print();




        env.execute();
    }
    public static List<String> mergeList(List<String> l1,List<String> l2){
        List<String> res = new ArrayList<>();
        res.addAll(l1);
        res.addAll(l2);
        return res;
    }
}
