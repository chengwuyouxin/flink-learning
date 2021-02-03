package com.lpq.stream.window;

import com.lpq.stream.model.MinMaxTemp;
import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/19
 */
public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        WindowedStream<SensorReading,String,TimeWindow> windowedStream =
                        input
                        .keyBy(value -> value.id)
                        .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        DataStream<MinMaxTemp> result =
                 input
                .keyBy(value -> value.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new HighAndLowTempProcessWindowFunction());

        result.print();

        env.execute("ProcessWindowFunction Demo!");
    }

    private static class HighAndLowTempProcessWindowFunction extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<SensorReading> iterable, Collector<MinMaxTemp> out) {
            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;

            for(SensorReading r : iterable){
                min = Math.min(min,r.temperature);
                max = Math.max(max,r.temperature);
            }

            Long windowEnd = context.window().getEnd();
            out.collect(new MinMaxTemp(key,min,max,windowEnd));
        }
    }
}





