package com.lpq.sql.wordcount;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class BatchTableDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);

        DataSet<Wordcount> input = env.fromElements(
                new Wordcount("hadoop",1),
                new Wordcount("spark",2)
        );

        //DataSet -> Table  两种方式
        tenv.createTemporaryView("inputtable",input,"word,frequency");
        Table inputtable = tenv.fromDataSet(input,"word,frequency");

        //SQL API
        Table result = tenv.sqlQuery("select word,sum(frequency) as frequency from inputtable group by word");

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
