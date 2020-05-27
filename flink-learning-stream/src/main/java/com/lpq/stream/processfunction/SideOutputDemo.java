package com.lpq.stream.processfunction;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author liupengqiang
 * @date 2020/5/21
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                        .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        SingleOutputStreamOperator<SensorReading> processed = input.process(new MySideOutput());

        // print the sideoutput
        processed.getSideOutput(new OutputTag<SensorReading>("MySideOutput"){}).print("SideOutput:");

        // print the main output
        processed.print("main output:");

        env.execute();
    }

    private static class MySideOutput extends ProcessFunction<SensorReading,SensorReading>{

        OutputTag<SensorReading> sideOutput;

        @Override
        public void open(Configuration parameters) throws Exception {
            sideOutput = new OutputTag<SensorReading>("MySideOutput"){};
        }

        @Override
        public void processElement(SensorReading r,
                                   Context context,
                                   Collector<SensorReading> out) throws Exception {
            //将温度高于65的发送到副输出，其余发送到主输出
            if(r.temperature > 65){
                //利用context将全部记录发送到副输出
                context.output(sideOutput, r);
            }else{
                //将数据发送到主输出
                out.collect(r);
            }



        }
    }
}

