package com.lpq.stream.state.operatorstate;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * @author liupengqiang
 * @date 2020/5/22
 *
 * 实现ListCheckpointed接口
 */
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000L);
        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        input.flatMap(new MyFlatMapFunction()).print();
        env.execute();
    }
    private static class MyFlatMapFunction extends RichFlatMapFunction<SensorReading, Tuple2<Integer,Long>>
        implements ListCheckpointed<Long>{

        private Double THRESHHOLD=50.0;
        private Long cnt=0L;

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<Integer, Long>> out) throws Exception {
            if(value.temperature>THRESHHOLD){
                cnt++;
            }
            int subTaskId=getRuntimeContext().getIndexOfThisSubtask();
            out.collect(Tuple2.of(subTaskId,cnt));
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(cnt);
        }

        //应用失败重启时调用
        @Override
        public void restoreState(List<Long> state) throws Exception {
            cnt=0L;
            for(Long i:state){
                cnt+=i;
            }
        }
    }
}
