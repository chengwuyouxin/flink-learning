package com.lpq.stream.window;

import com.lpq.stream.model.MinMaxTemp;
import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/19
 *
 * 如果需要增量聚合，同时还需要访问窗口元数据，则可以组合使用增量聚合函数和全量窗口函数
 */
public class ReduceAndProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<MinMaxTemp> result =
                input
                        .map(new MapFunction<SensorReading, Tuple3<String,Double,Double>>() {

                            @Override
                            public Tuple3<String, Double, Double> map(SensorReading sensorReading) throws Exception {
                                return Tuple3.of(sensorReading.id,sensorReading.temperature,sensorReading.temperature);
                            }
                        })
                        .keyBy(value -> value.f0)
                        .timeWindow(Time.seconds(5))
                        .reduce(new ReduceFunction<Tuple3<String,Double,Double>>() {
                            @Override
                            public Tuple3<String,Double,Double> reduce(Tuple3<String,Double,Double> r1, Tuple3<String,Double,Double> r2) throws Exception {
                                return Tuple3.of(r1.f0,Math.min(r1.f1,r2.f1),Math.max(r1.f2,r2.f2));
                            }
                        }, new MyProcessWindowFunction());

        result.print();

        env.execute("ReduceFunction and ProcessWindowFunction Demo");
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String,Double,Double>,MinMaxTemp,String, TimeWindow>{

        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Double, Double>> iterable, Collector<MinMaxTemp> out) throws Exception {
            Tuple3<String,Double,Double> t1 = iterable.iterator().next();
            Long endTS = context.window().getEnd();
            out.collect(new MinMaxTemp(key,t1.f1,t1.f2,endTS));
        }
    }
}

