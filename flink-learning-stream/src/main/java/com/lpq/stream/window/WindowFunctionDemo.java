package com.lpq.stream.window;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author liupengqiang
 * @date 2020/5/28
 *
 * WindowFunction对整个窗口的元素进行处理
 * 本例对窗口中的元素的个数进行统计
 */
public class WindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Integer> res = input.keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .apply(new MyWindowFunction());

        res.print();
        env.execute();
    }

    private static class MyWindowFunction
            implements WindowFunction<SensorReading,Integer,String, TimeWindow>{

        @Override
        public void apply(String key, TimeWindow timeWindow,
                          Iterable<SensorReading> iterable, Collector<Integer> out) throws Exception {
            Iterator<SensorReading> iterator = iterable.iterator();
            int size = 0;
            while(iterator.hasNext()){
                size++;
                iterator.next();
            }
            out.collect(size);
        }
    }
}
