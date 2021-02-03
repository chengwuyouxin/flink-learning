package com.lpq.stream.join;

import com.lpq.connector.utils.KafkaUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author liupengqiang
 * @date 2020/6/11
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties kafkaConsumerProperties = KafkaUtil.getKafkaConsumerProperties("JoinDemo");
        FlinkKafkaConsumer011 consumer011_order = new FlinkKafkaConsumer011(
                "order",
                new OrderDeserialization(),
                kafkaConsumerProperties
        );
        FlinkKafkaConsumer011 consumer011_rate = new FlinkKafkaConsumer011(
                "rate",
                new RateDeserialization(),
                kafkaConsumerProperties
        );

        DataStream<Tuple4<Long,String,String,Integer>> orderstream =
                env.addSource(consumer011_order)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Object element) {
                        return ((Tuple4<Long,String,String,Integer>)element).f0;
                    }
                });
        DataStream<Tuple3<Long,String,Integer>> ratestream =
                env.addSource(consumer011_rate)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Object element) {
                        return ((Tuple3<Long,String,Integer>)element).f0;
                    }
                });

//        orderstream.print("order");
//        ratestream.print("rate");

        DataStream<Tuple4<String,String,String,String>> res = orderstream.join(ratestream)
                .where(new KeySelector<Tuple4<Long, String, String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple4<Long, String, String, Integer> value) throws Exception {
                        return value.f2;
                    }
                })
                .equalTo(new KeySelector<Tuple3<Long, String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple3<Long, String, Integer> value) throws Exception {
                        return value.f1;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple4<Long, String, String, Integer>, Tuple3<Long, String, Integer>,
                        Tuple4<String,String,String,String>>() {


                    @Override
                    public Tuple4<String, String, String, String> join(Tuple4<Long, String, String, Integer> first,
                                                                       Tuple3<Long, String, Integer> second) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
                        String order_time = sdf.format(new Date(first.f0));
                        String rate_time = sdf.format(new Date(second.f0));
                        return Tuple4.of(order_time,first.f2,rate_time,second.f1);
                    }
                });
        res.print();

        env.execute();

    }
    private static class RateDeserialization implements DeserializationSchema<Tuple3<Long,String,Integer>>{
        @Override
        public Tuple3<Long, String, Integer> deserialize(byte[] message) throws IOException {
            String record = new String(message);
            String[] ra = record.split(",");
            return Tuple3.of(Long.valueOf(ra[0]),ra[1],Integer.valueOf(ra[2]));
        }

        @Override
        public boolean isEndOfStream(Tuple3<Long, String, Integer> nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Tuple3<Long, String, Integer>> getProducedType() {
            return TypeInformation.of(new TypeHint<Tuple3<Long,String,Integer>>(){});
        }
    }
    private static class OrderDeserialization implements DeserializationSchema<Tuple4<Long,String,String,Integer>>{

        @Override
        public Tuple4<Long, String, String, Integer> deserialize(byte[] message) throws IOException {
            String record = new String(message);
            String[] ra = record.split(",");
            return Tuple4.of(Long.valueOf(ra[0]),ra[1],ra[2],Integer.valueOf(ra[3]));
        }

        @Override
        public boolean isEndOfStream(Tuple4<Long, String, String, Integer> nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Tuple4<Long, String, String, Integer>> getProducedType() {
            return TypeInformation.of(
                    new TypeHint<Tuple4<Long,String,String,Integer>>(){}
            );
        }
    }
}
