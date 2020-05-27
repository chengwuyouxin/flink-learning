package com.lpq.stream.transformation.connect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author liupengqiang
 * @date 2020/5/26
 */

public class ConnectCoMapFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据源
        CustomSourceFunction sourceFunction = new CustomSourceFunction();
        DataStreamSource<Tuple2<String, Long>> customDS = env.addSource(sourceFunction);

        // 第一份数据
        DataStream<Tuple2<String, Long>> filter1DS = customDS
                .filter(value -> "a".equals(value.f0));
        // 第二份数据
        DataStream<Long> filter2DS = customDS
                .filter(value -> "b".equals(value.f0))
                .map(value -> value.f1);

//        filter1DS.print("filter1");
//        filter2DS.print("filter2");

        // 连接2份数据
        DataStream<Tuple3<String, Long, Long>> connectDS =
                filter1DS
                .connect(filter2DS)
                .map(new CoMapFunction<Tuple2<String, Long>, Long, Tuple3<String, Long, Long>>() {

                    AtomicLong num = new AtomicLong(); // 单个Slot内

                    @Override
                    public Tuple3<String, Long, Long> map1(Tuple2<String, Long> value) throws Exception {
                        return Tuple3.of(value.f0, value.f1, num.incrementAndGet());
                    }

                    @Override
                    public Tuple3<String, Long, Long> map2(Long value) throws Exception {
                        return Tuple3.of("value", value, num.incrementAndGet());
                    }
                });

        DataStream<String> resultDS = connectDS
                .map(value -> value.f0 + "|" + value.f1 + "|" + value.f2);

        resultDS.print();

            env.execute();
    }

    private static class CustomSourceFunction extends RichSourceFunction<Tuple2<String, Long>> {

        private boolean flag = true;

        @Override
        public void run(SourceFunction.SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            List<String> data = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
            Random random = new Random();
            while (flag) {
                Thread.sleep(100);
                // 随机取一个值
                String key = data.get(random.nextInt(data.size()));
                long value = System.currentTimeMillis();
                ctx.collect(Tuple2.of(key, value));
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

    }

}
