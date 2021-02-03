package com.lpq.stream.window;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import com.lpq.stream.util.TimeUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author liupengqiang
 * @date 2020/5/22
 */
public class TimeWindowDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        //每5秒中处理一次，窗口的截止时间和水位线是一样的，都比当前时间慢5秒
        //SensorTimeAssigner设置的容忍度是5秒，因此当前记录最大时间戳为15：13：50时，
        //处理的时间窗口为 15：13：40-45秒
        DataStream<Tuple4<String,String,String,String>> res =
                input.keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                })
                //默认启用的是TumblingWindow
                .timeWindow(Time.seconds(5))
                .process(new MyProcessWindowFunction());

        res.print();
        env.execute("TumblingWindowDemo");
    }

    private static class MyProcessWindowFunction extends
            ProcessWindowFunction<SensorReading, Tuple4<String,String,String,String>,String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<SensorReading> iterable,
                            Collector<Tuple4<String, String, String, String>> out) throws Exception {

            String id = iterable.iterator().next().id;
            String winstart = TimeUtil.unix2Str(context.window().getStart());
            String winend = TimeUtil.unix2Str(context.window().getEnd());
            String processingtime = TimeUtil.unix2Str(context.currentProcessingTime());
            String watermark = TimeUtil.unix2Str(context.currentWatermark());
            out.collect(Tuple4.of(id,winend,watermark,processingtime));
        }


    }


}
