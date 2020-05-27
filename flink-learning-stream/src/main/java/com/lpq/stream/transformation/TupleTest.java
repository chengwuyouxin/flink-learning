package com.lpq.stream.transformation;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author liupengqiang
 * @date 2020/5/26
 */
public class TupleTest {
    public static void main(String[] args) {
        Tuple2<String,Integer> t = Tuple2.of("lpq",23);
        t.f0 = "xmm";
        t.f1 = 31;
        System.out.println(t);
    }
}
