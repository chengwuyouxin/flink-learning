package com.lpq.sql.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Collection Source
 *
 */

public class CollectionSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

//        1、fromCollection(Collection) - 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同。
//        DataStream<WC> words = env.fromCollection(Arrays.asList(
//              new WC("Hello",1),
//              new WC("Flink",1)
//        ));

//        2、fromCollection(Iterator, Class) - 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。

//        3、fromElements(T …) - 从给定的对象序列中创建数据流。所有对象类型必须相同。
        DataStream<WC> words = env.fromElements(
                new WC("Hello",1),
                new WC("Flink",1)
        );

//        4、fromParallelCollection(SplittableIterator, Class) - 从一个迭代器中创建并行数据流。Class 指定了该迭代器返回元素的类型。

//        5、generateSequence(from, to) - 创建一个生成指定区间范围内的数字序列的并行数据流。

        words.print();
        env.execute("Collection Source");
    }
    static class WC{
        private String word;
        private int frequency;

        public WC(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
}
