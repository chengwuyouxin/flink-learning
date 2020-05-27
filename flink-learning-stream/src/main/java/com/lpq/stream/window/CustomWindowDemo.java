package com.lpq.stream.window;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import com.lpq.stream.util.TimeUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.util.Collection;
import java.util.Collections;

/**
 * @author liupengqiang
 * @date 2020/5/22
 */
public class CustomWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple5<String,String,String,String,Integer>> res =
                input.keyBy(r -> r.id)
                .window(new MyWindowsAssigner(4000))
                .trigger(new OneSecondIntervalTrigger())
                .process(new MyCountFunction());

        res.print();

        env.execute("Custom window demo");

    }

    private static class MyWindowsAssigner extends
            WindowAssigner<Object, TimeWindow>{
        private long windowSize;

        public MyWindowsAssigner(long windowSize){
            this.windowSize = windowSize;
        }

        @Override
        public Collection<TimeWindow> assignWindows(Object o, long ts,
                        WindowAssignerContext ctx) {
            long startTime = ts - (ts % windowSize);
            long endTime = startTime + windowSize;
            return Collections.singletonList(new TimeWindow(startTime,endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(
                StreamExecutionEnvironment streamExecutionEnvironment) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(
                ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }

    private static class OneSecondIntervalTrigger extends
            Trigger<SensorReading,TimeWindow>{

        //每当有元素添加到窗口时都会调用
        @Override
        public TriggerResult onElement(SensorReading sensorReading,
           long timeStamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> firstSeen = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("firstSeen", Types.BOOLEAN)
            );

            if(null == firstSeen.value()){
                long t = triggerContext.getCurrentWatermark() +
                        (1000 - (triggerContext.getCurrentWatermark() % 1000));
                triggerContext.registerEventTimeTimer(t);
                triggerContext.registerEventTimeTimer(timeWindow.getEnd());
                firstSeen.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        //处理时间计时器触发时调用
        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow,
                          TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //事件时间计时器出发时调用
        @Override
        public TriggerResult onEventTime(long timeStamp, TimeWindow timeWindow,
                         TriggerContext triggerContext) throws Exception {
            if(timeStamp == timeWindow.getEnd()){
                return TriggerResult.FIRE_AND_PURGE;
            }else{
                long t = triggerContext.getCurrentWatermark() +
                        (1000 - (triggerContext.getCurrentWatermark() % 1000));
                if(t < timeWindow.getEnd()){
                    triggerContext.registerEventTimeTimer(t);
                }
                return TriggerResult.FIRE;
            }
        }

        //在清除窗口时调用
        @Override
        public void clear(TimeWindow timeWindow,
                  TriggerContext triggerContext) throws Exception {

        }
    }

    private static class MyCountFunction
        extends ProcessWindowFunction<SensorReading,
            Tuple5<String,String,String,String,Integer>,String, TimeWindow>{


        @Override
        public void process(String key, Context ctx, Iterable<SensorReading> iterable,
            Collector<Tuple5<String,String, String, String, Integer>> out) throws Exception {
            int cnt = 0;
            for(SensorReading r : iterable){
                cnt++;
            }
            String winend = TimeUtil.unix2Str(ctx.window().getEnd());
            String watermark = TimeUtil.unix2Str(ctx.currentWatermark());
            String processingtime = TimeUtil.unix2Str(ctx.currentProcessingTime());
            out.collect(Tuple5.of(key,winend,watermark,processingtime,cnt));
        }
    }
}
