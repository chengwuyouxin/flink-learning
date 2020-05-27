package com.lpq.sql.wordcount;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class SQLBatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);

        DataSet<Wordcount> input = env.fromElements(
                new Wordcount("hadoop",1),
                new Wordcount("spark",2)
        );

        //SQL API
        //register DataSet to Table 通过这种方式定义的table可以之间在sql语句中使用
//        tenv.createTemporaryView("inputtable",input,"word, frequency");
//        Table result = tenv.sqlQuery("select word,sum(frequency) as frequency from inputtable group by word");

        //Table API
        //convert Dataset to Table 通过这种方式定义的table不能在sql语句中直接使用
        Table inputtable = tenv.fromDataSet(input,"word,frequency");
        Table result = inputtable.groupBy("word")
                .select("word,frequency.sum as frequency")
                .filter("frequency >= 1");

        DataSet<Wordcount> output = tenv.toDataSet(result, Wordcount.class);

        output.print();
    }

    /**
     * Simple POJO containing a word and its respective count.
     */
    public static class Wordcount {
        public String word;
        public long frequency;

        // public constructor to make it a Flink POJO
        public Wordcount() {}

        public Wordcount(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency;
        }
    }
}
