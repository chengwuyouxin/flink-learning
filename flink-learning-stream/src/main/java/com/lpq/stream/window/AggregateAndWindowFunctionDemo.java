package com.lpq.stream.window;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/28
 *
 *
 */
public class AggregateAndWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                        .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        //MyAggregationFunction产生的结果输出到MyWindowFunction中进行下一步的处理
        //每5秒处理一次，每个窗口产生每个温度计的读书个数
        DataStream<Tuple2<String,Long>> res =
                input
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAggregateFunction(),new MyWindowFunction());

        res.print();

        env.execute();
    }

    private static class MyAggregateFunction
            implements AggregateFunction<SensorReading,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SensorReading value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    private static class MyWindowFunction implements
            WindowFunction<Long, Tuple2<String,Long>,String, TimeWindow>{

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
            out.collect(Tuple2.of(key,input.iterator().next()));
        }
    }
}
