package com.lpq.stream.processlaterecord;

import com.alibaba.fastjson.JSON;
import com.lpq.connector.dao.ConsoleRecord;
import com.lpq.connector.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;


/**
 * @author liupengqiang
 * @date 2020/6/10
 *
 * 本例演示了利用迟到的记录更新已产生的结果
 * allowedLateness(Time.seconds(5)) 设置等待迟到的时间为5秒，当超过watermark5秒后，
 * 窗口的计算结果不再保留，也就无法更新
 * 注意由于本身watermark允许记录迟到5秒，因此一共允许记录迟到10秒
 *
 */
public class UpdateResult {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer011<String> consumer011 =
                new FlinkKafkaConsumer011<>(
                    "consolerecord",
                    new SimpleStringSchema(),
                    KafkaUtil.getKafkaConsumerProperties("UpdateResult")
                );

        consumer011.setStartFromGroupOffsets();

        DataStream<ConsoleRecord> input = env.addSource(consumer011)
                .map(new MapFunction<String, ConsoleRecord>() {
                    @Override
                    public ConsoleRecord map(String value) throws Exception {
                        return JSON.parseObject(value,ConsoleRecord.class);
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ConsoleRecord>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(ConsoleRecord record) {
                        return record.getTimestamp();
                    }
                });

        DataStream<Tuple6<Integer,String,String,String,Integer,String>> res =
                input
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new UpdatdingProcessWindowFunction());

        res.print();


        env.execute();
    }


    private static class UpdatdingProcessWindowFunction extends
            ProcessWindowFunction<ConsoleRecord,Tuple6<Integer,String,String,String,Integer,String>,Integer, TimeWindow>{

        @Override
        public void process(Integer key, Context context, Iterable<ConsoleRecord> elements,
                            Collector<Tuple6<Integer,String,String,String,Integer,String>> out) throws Exception {
            int cnt = 0;
            Iterator<ConsoleRecord> iterator = elements.iterator();
            while(iterator.hasNext()){
                cnt++;
                iterator.next();
            }

            ValueState isUpdate = context.windowState().getState(
                    new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN)
            );
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

            String currentWaterMark = sdf.format(new Date(context.currentWatermark()));
            String processingTime = sdf.format(new Date(context.currentProcessingTime()));
            String windowEndTime = sdf.format(new Date(context.window().getEnd()));

            if(isUpdate.value() == null){
                out.collect(Tuple6.of(key,currentWaterMark,processingTime,windowEndTime,cnt,"first"));
                isUpdate.update(true);
            }else{
                out.collect(Tuple6.of(key,currentWaterMark,processingTime,windowEndTime,cnt,"update"));
            }
        }
    }
}
