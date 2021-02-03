package com.lpq.stream.window.trigger;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * @author liupengqiang
 * @date 2021/1/28
 */
public class DeltaTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<SensorReading> input=env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        //此处的window有两个Trigger
        input.keyBy(r->r.id)
                //创建窗口时添加的trigger时EventTimeTrigger
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                //当后一个温度比前一个温度增加超过threshhold的时候会触发窗口计算
                .trigger(DeltaTrigger.of(
                        20,
                        new DeltaFunction<SensorReading>() {
                            @Override
                            public double getDelta(SensorReading oldDataPoint, SensorReading newDataPoint) {
                                return newDataPoint.temperature-oldDataPoint.temperature;
                            }
                        },
                        Types.POJO(SensorReading.class).createSerializer(env.getConfig())))
                .apply(new WindowFunction<SensorReading, Tuple3<String,String,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SensorReading> input,
                                      Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        int cnt=0;
                        Iterator<SensorReading> iterator=input.iterator();
                        while(iterator.hasNext()){
                            iterator.next();
                            cnt++;
                        }
                        Date winStart=new Date(window.getStart());
                        SimpleDateFormat sdf=new SimpleDateFormat("HH:mm:ss");
                        out.collect(Tuple3.of(key,sdf.format(winStart),cnt));
                    }
                })
                .print();
        env.execute();
    }
}
