package com.lpq.stream.join;

import com.lpq.connector.utils.KafkaUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author liupengqiang
 * @date 2020/6/11
 */
public class TopNDemo {
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

        // timestamp,product,money
        DataStream<Tuple3<Long,String,Integer>> joinedStream = orderstream.join(ratestream)
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
                        Tuple3<Long,String,Integer>>() {
                    @Override
                    public Tuple3<Long,String,Integer> join(Tuple4<Long, String, String, Integer> first,
                                                                       Tuple3<Long, String, Integer> second) throws Exception {
                        return Tuple3.of(first.f0,first.f1,first.f3 * second.f2);
                    }
                });

//        joinedStream.print("JoinedStream:");

        //product,windowEndTime,income
        DataStream<ProductOrder> windowedData = joinedStream.keyBy(r -> r.f1)
                .timeWindow(Time.seconds(10),Time.seconds(10))
                .aggregate(new Myagg(),new MyWindowFunction());
//        windowedData.print("windowed data:");

        DataStream<String> res = windowedData
                .keyBy(r -> r.windowEnd)
                .process(new MyKeyedProcessFunction(3));
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
    private static class Myagg implements AggregateFunction<Tuple3<Long,String,Integer>,
            Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<Long, String, Integer> value, Integer accumulator) {
            return accumulator + value.f2;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    private static class ProductOrder{
        private String product;
        private Long windowEnd;
        private Integer income;

        public ProductOrder() {
        }

        public ProductOrder(String product, Long windowEnd, Integer income) {
            this.product = product;
            this.windowEnd = windowEnd;
            this.income = income;
        }

        public String getProduct() {
            return product;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public Long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(Long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public Integer getIncome() {
            return income;
        }

        public void setIncome(Integer income) {
            this.income = income;
        }

        @Override
        public String toString() {
            SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss");
            String windowEndTime = sdf.format(new Date(windowEnd));
            return "ProductOrder{" +
                    "product='" + product + '\'' +
                    ", windowEnd=" + windowEndTime +
                    ", income=" + income +
                    '}';
        }
    }

    private static class MyWindowFunction implements WindowFunction<
                Integer,ProductOrder,String, TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window,
                          Iterable<Integer> input,
                          Collector<ProductOrder> out) throws Exception {
            Integer amount = input.iterator().next();
            out.collect(new ProductOrder(s,window.getEnd(),amount));
        }
    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<Long, ProductOrder, String> {
        private int topSize;
        private ListState<ProductOrder> listState;

        public MyKeyedProcessFunction(int topSize){
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ProductOrder> listStateDescriptor =
                    new ListStateDescriptor<ProductOrder>("productorder-state",ProductOrder.class);
            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(ProductOrder value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ProductOrder> allProducts = new ArrayList<>();
            listState.get().forEach(productOrder -> allProducts.add(productOrder));
            listState.clear();
            allProducts.sort(new Comparator<ProductOrder>() {
                @Override
                public int compare(ProductOrder o1, ProductOrder o2) {
                    return (int)(o2.income-o1.income);
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("==================================\n");
            for(int i=0;i<topSize && i<allProducts.size();i++){
                sb.append("No " + i + "："+allProducts.get(i).toString()+"\n");
            }
            sb.append("==================================\n");
            out.collect(sb.toString());
        }
    }
}
