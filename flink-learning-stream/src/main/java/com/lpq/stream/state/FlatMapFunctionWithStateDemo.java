package com.lpq.stream.state;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author liupengqiang
 * @date 2020/5/20
 */
public class FlatMapFunctionWithStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple3<String,Double,Double>> result =
                input
                .keyBy(value -> value.id)
                .flatMap(new MyFlatMapFunctionWithState(10));

        result.print();

        env.execute();
    }

    //温度上升超过10度时报警
    private static class MyFlatMapFunctionWithState extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>> {
        private double threshold;
        private ValueState<Double> lastTempState;
        public MyFlatMapFunctionWithState(double threshold){
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE));
        }

        @Override
        public void flatMap(SensorReading r, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if(lastTemp != null){
                Double tempDiff = Math.abs(r.temperature - lastTemp);
                if(tempDiff > threshold){
                    out.collect(Tuple3.of(r.id,r.temperature,lastTemp));
                }
            }
            this.lastTempState.update(r.temperature);
        }
    }
}


