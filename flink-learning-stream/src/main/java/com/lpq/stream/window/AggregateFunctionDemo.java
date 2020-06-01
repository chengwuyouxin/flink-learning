package com.lpq.stream.window;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liupengqiang
 * @date 2020/5/19
 *
 * AggregationFunction使用
 * 计算每个传感器的平均温度
 */
public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //使用周期性水位分配器，默认为每隔200ms发出一次水位线
//        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> input =
                env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedDemo());

        DataStream<Tuple2<String,Double>> result = input
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                        return Tuple2.of(sensorReading.id,sensorReading.temperature);
                    }
                })
                .keyBy("f0")
                .timeWindow(Time.seconds(5))
                .aggregate(new AvgTempFunction());
        result.print();
        env.execute("Aggregation Function Demo");

    }
}

class AvgTempFunction implements AggregateFunction<
        Tuple2<String,Double>, Tuple3<String,Double,Integer>,Tuple2<String,Double>>{

    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return Tuple3.of("",0.0,0);
    }

    @Override
    public Tuple3<String, Double, Integer> add(Tuple2<String, Double> in, Tuple3<String, Double, Integer> acc) {

        return Tuple3.of(in.f0,in.f1+acc.f1,acc.f2+1);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
        return Tuple2.of(acc.f0,acc.f1/acc.f2);
    }

    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1, Tuple3<String, Double, Integer> acc2) {
        return Tuple3.of(acc1.f0,acc1.f1+acc2.f1,acc1.f2+acc2.f2);
    }
}