package com.lpq.stream.state.operatorstate;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import com.lpq.stream.source.sensor.ThresholdUpdate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author liupengqiang
 * @date 2021/2/3
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);

        DataStream<SensorReading> sensor=env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

//        DataStream<ThresholdUpdate> thresholds=env.fromElements(
//                new ThresholdUpdate("sensor_1",10.0),
//                new ThresholdUpdate("sensor_2",20.0)
//        );

        //在windows命令行窗口输入命令 nc -l -p 12345 即可实时更新threshhold
        DataStream<ThresholdUpdate> thresholds=env.socketTextStream("localhost",12345)
                .map(new MapFunction<String, ThresholdUpdate>() {

                    @Override
                    public ThresholdUpdate map(String value) throws Exception {
                        String[] strs=value.split(",");
                        return new ThresholdUpdate(strs[0],Double.valueOf(strs[1]));
                    }
                });

        MapStateDescriptor<String,Double> mapStateDescriptor=new MapStateDescriptor<String, Double>(
                "threshold",
                Types.STRING,
                Types.DOUBLE);

        BroadcastStream<ThresholdUpdate> broadcastStream = thresholds.broadcast(mapStateDescriptor);

        sensor.keyBy(r -> r.id)
                .connect(broadcastStream)
                .process(new AlertFunction()).print();

        env.execute();

    }
    private static class AlertFunction extends KeyedBroadcastProcessFunction<
            String,SensorReading,ThresholdUpdate, Tuple3<String,Double,Double>>{
        ValueStateDescriptor valueStateDescriptor=new ValueStateDescriptor("lastTemp",Types.DOUBLE);
        ValueState<Double> lastTemp;

        MapStateDescriptor<String,Double> mapStateDescriptor=new MapStateDescriptor<String, Double>(
                "threshold",
                Types.STRING,
                Types.DOUBLE
        );

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp=this.getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(SensorReading value, ReadOnlyContext ctx, Collector<Tuple3<String, Double, Double>> out)
                throws Exception {
            ReadOnlyBroadcastState<String,Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            if(broadcastState.contains(value.id)){
                Double tempDiff=Math.abs(value.temperature-lastTemp.value());
                if(tempDiff>broadcastState.get(value.id)){
                    out.collect(Tuple3.of(value.id,value.temperature,tempDiff));
                }
            }
            lastTemp.update(value.temperature);
        }

        @Override
        public void processBroadcastElement(ThresholdUpdate value, Context ctx, Collector<Tuple3<String, Double, Double>> out)
                throws Exception {
            BroadcastState<String, Double> broadcastState=ctx.getBroadcastState(mapStateDescriptor);
            if(value.getThreshold()!=0.0){
                broadcastState.put(value.getId(),value.getThreshold());
            }else{
                broadcastState.remove(value.getId());
            }
        }
    }
}

