package com.lpq.stream.type;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author liupengqiang
 * @date 2020/5/26
 *
 * 显示指定TypeInformation的两种方式
 * 1.实现ResultTypeQueryable接口
 * 2.通过return算子指明
 */
public class TypeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> res =
                env
                .fromElements(
                    Tuple2.of("lpq",23),
                    Tuple2.of("xmm",21)
                )
//                .map(new MyMapper());
                .map(t -> new Student(t.f0,t.f1))
                .returns(Types.POJO(Student.class));
        res.print();
        env.execute();
    }

    private static class MyMapper implements
            MapFunction<Tuple2<String,Integer>,Student>, ResultTypeQueryable<Student>{

        @Override
        public Student map(Tuple2<String, Integer> value) throws Exception {
            return new Student(value.f0,value.f1);
        }

        @Override
        public TypeInformation<Student> getProducedType() {
            return Types.POJO(Student.class);
        }
    }
}


