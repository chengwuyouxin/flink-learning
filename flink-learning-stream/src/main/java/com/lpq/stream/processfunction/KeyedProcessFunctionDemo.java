package com.lpq.stream.processfunction;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author liupengqiang
 * @date 2020/5/19
 *
 *
 */
public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<String> result =
                input.keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                })
                .process(new TempIncreaseAlertFunction());

        result.print();

        env.execute();
    }

    //如果一个传感器在3秒内温度一直增加，则会报警
    private static class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {

        //存储最近一次传感器的温度
        private ValueState<Double> lastTemp;
        //存储当前活动计时器的时间戳
        private ValueState<Long> currentTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE));

            currentTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer",Types.LONG));
        }

        @Override
        public void processElement(SensorReading r, Context context, Collector<String> out) throws Exception {

            //获取前一个记录的温度
            Double preTemp = lastTemp.value();

            //获取当前计时器的时间戳
            Long currentTimeStamp = currentTimer.value();

            //更新最近一次的温度
            lastTemp.update(r.temperature);

            //第一条记录处理时之前的温度时null
            if(preTemp == null){
                return;
            }

            //如果温度是下降的，删除计时器，并且删除currentTimer
            if(preTemp > r.temperature){
                if(currentTimeStamp != null){
                    context.timerService().deleteEventTimeTimer(currentTimeStamp);
                    currentTimer.clear();
                }
            }else if(r.temperature >= preTemp && currentTimeStamp == null){
                //3秒中之内可能有多次温度增加，只有在第一次温度增加时注册计时器
                Long timeTs = context.timerService().currentWatermark() + 3000L;
                context.timerService().registerEventTimeTimer(timeTs);
                //保存计时器出发的时间
                currentTimer.update(timeTs);
            }
        }

        @Override
        public void onTimer(long timertimestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
            out.collect("Temperature of sensor '" + ctx.getCurrentKey() + "' monotonically increased for 3 seconds!"+
                    "currentProcessingTime:" + sdf.format(new Date(ctx.timerService().currentProcessingTime()))
            + ",currentWaterMark:" + sdf.format(new Date(ctx.timerService().currentWatermark()))
            + ",timerTime:" + sdf.format(new Date(timertimestamp )));
            //报警之后清除计时器，重新开始计时
            currentTimer.clear();
        }
    }

//    private static class MyProcessFunction extends ProcessFunction<SensorReading, Tuple4<String,Double,String,String>> {
//
//        @Override
//        public void processElement(SensorReading r, Context ctx, Collector<Tuple4<String,Double,String,String>> out) throws Exception {
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
//            String waterMark = sdf.format(new Date(ctx.timerService().currentWatermark()));
//            String timeStamp = sdf.format(new Date(r.timestamp));
//            out.collect(Tuple4.of(r.id,r.temperature,timeStamp,waterMark));
//
//        }
//    }
}
