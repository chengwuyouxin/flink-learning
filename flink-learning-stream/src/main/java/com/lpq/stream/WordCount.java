package com.lpq.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * @author liupengqiang
 * @date 2020/5/9
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> wordsDS = env.fromElements(WORDS).flatMap((String line, Collector<String> ctx) -> {
            // 切分单词
            Arrays.stream(line.split(" ")).forEach(word -> {
                ctx.collect(word);
            });
        }).returns(Types.STRING);

        //将单词转换为一个个的元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = wordsDS
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 3.3 按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedDS = tupleDS.keyBy(tuple -> tuple.f0);

        // 3.4 对每组单词数量进行累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = keyedDS
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

        resultDS.print();

        env.execute("Word Count using stream api");
    }

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come,",
            "When we have shuffled off this mortal coil,",
            "Must give us pause: there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would these fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };
}
