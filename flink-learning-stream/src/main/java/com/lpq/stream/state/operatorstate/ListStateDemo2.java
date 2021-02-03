package com.lpq.stream.state.operatorstate;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * @author liupengqiang
 * @date 2021/1/28
 *
 * checkpoint未成功
 */
public class ListStateDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        DataStream<SensorReading> input=env.addSource(new SensorSource())
                                            .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        input.flatMap(new MyFlatMapFunction()).print();
        env.execute();
    }
    private static class MyFlatMapFunction extends RichFlatMapFunction<SensorReading, Tuple2<Integer,Long>>
        implements CheckpointedFunction{

        private double Threshhold=50.0;
        private ListState<Long> longListState;
        private Long localcnt=0L;

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<Integer, Long>> out) throws Exception {
            int subTaskId=getRuntimeContext().getIndexOfThisSubtask();
            if(value.temperature>Threshhold){
                localcnt++;
            }
            out.collect(Tuple2.of(subTaskId,localcnt));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            longListState.clear();
            longListState.add(localcnt);
        }

        //应用初始化或者失败后重启时调用
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> listStateDescriptor=new ListStateDescriptor(
                "highTempCnt",
                Types.LONG
            );
            longListState=context.getOperatorStateStore().getListState(listStateDescriptor);
            if(context.isRestored()){
                for(Long temperature:longListState.get()){
                    localcnt+=temperature;
                }
            }

//            for(Long temperature:longListState.get()){
//                localcnt+=temperature;
//            }
        }
    }
}
