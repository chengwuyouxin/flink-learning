package com.lpq.stream.transformation;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liupengqiang
 * @date 2021/1/22
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> sensorRecords= env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());


        SingleOutputStreamOperator<SensorReading> mainStream = sensorRecords.process(new ProcessFunction<SensorReading, SensorReading>() {
            //id是后续获取sideoutput的重要标识
            OutputTag<SensorReading> outputTag1=new OutputTag<SensorReading>("side-output-1"){};
            OutputTag<SensorReading> outputTag2=new OutputTag<SensorReading>("side-output-2"){};
            OutputTag<SensorReading> outputTag3=new OutputTag<SensorReading>("side-output-3"){};

            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if(value.id.equals("sensor_1")){
                    ctx.output(outputTag1,value);
                }else if(value.id.equals("sensor_2")){
                    ctx.output(outputTag2,value);
                }else{
                    ctx.output(outputTag3,value);
                }
            }
        });

        mainStream.getSideOutput(new OutputTag<SensorReading>("side-output-1"){}).print("side-output-1");
        mainStream.getSideOutput(new OutputTag<SensorReading>("side-output-2"){}).print("side-output-2");
        mainStream.getSideOutput(new OutputTag<SensorReading>("side-output-3"){}).print("side-output-3");

        env.execute();
    }
}
