/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lpq.stream.processfunction;


import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * This example shows how to use a CoProcessFunction and Timers.
 */
public class CoProcessFunctionTimers {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // switch messages disable filtering of sensor readings for a specific amount of time
        DataStream<Tuple2<String, Long>> filterSwitches = env
            .fromElements(
                // forward readings of sensor_2 for 10 seconds
                Tuple2.of("sensor_2", 10_000L),
                // forward readings of sensor_7 for 1 minute
                Tuple2.of("sensor_7", 20_000L));

        // ingest sensor stream
        DataStream<SensorReading> readings = env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource());

        DataStream<SensorReading> forwardedReadings = readings
            // connect readings and switches
            .connect(filterSwitches)
            // key by sensor ids
            .keyBy(r -> r.id, s -> s.f0)
            // apply filtering CoProcessFunction
            .process(new ReadingFilter());

        forwardedReadings.print();

        env.execute("Filter sensor readings");
    }

    public static class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        // switch to enable forwarding
        private ValueState<Boolean> forwardingEnabled;
        // timestamp to disable the currently active timer
        private ValueState<Long> disableTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            forwardingEnabled = getRuntimeContext().getState(
                new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN));
            disableTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<Long>("timer", Types.LONG));
        }


        //只有sensor_2和sensor_7才有匹配的记录进入到processElement2方法，打开forwardEnable开关一定的时间
        //因此输出结果中只有sensor_2和sensor_7的数据
        @Override
        public void processElement1(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
            // check if we need to forward the reading
            Boolean forward = forwardingEnabled.value();
            if (forward != null && forward) {
                collector.collect(sensorReading);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> stringLongTuple2, Context context, Collector<SensorReading> collector) throws Exception {
            // enable forwarding of readings
            forwardingEnabled.update(true);
            // set timer to disable switch
            long timerTimestamp = context.timerService().currentProcessingTime() + stringLongTuple2.f1;
            Long curTimerTimestamp = disableTimer.value();
            if (curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
                // remove current timer
                if (curTimerTimestamp != null) {
                    context.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                }
                // register new timer
                context.timerService().registerProcessingTimeTimer(timerTimestamp);
                disableTimer.update(timerTimestamp);
            }
        }

        @Override
        public void onTimer(long ts, CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading>.OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            // remove all state
            forwardingEnabled.clear();
            disableTimer.clear();
        }

    }
}


