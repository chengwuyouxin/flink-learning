package com.lpq.sql.wordcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

public class StreamDemo {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String planner = parameterTool.has("planner")?parameterTool.get("planner"):"flink";

        //set up execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //create table execution environment use blink or flink planner
        StreamTableEnvironment tEnv;
        if(Objects.equals(planner,"blink")){
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();
            tEnv = StreamTableEnvironment.create(env,settings);
        }else if(Objects.equals(planner,"flink")){
            tEnv = StreamTableEnvironment.create(env);
        }else{
            System.out.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', \" +\n" +
                    "\t\t\t\t\"where planner (it is either flink or blink, and the default is flink) indicates whether the \" +\n" +
                    "\t\t\t\t\"example uses flink planner or blink planner.");
            return;
        }

        //env.fromElements
//        DataStream<Order> orderA = env.fromElements(
//                new Order(1L,"water",2),
//                new Order(2L,"coffee",3)
//        );
//
//        DataStream<Order> orderB = env.fromElements(
//                new Order(1L,"phone",1),
//                new Order(3L,"pencil",5)
//        );

        //env.fromCollection
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        //convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA,"user,product,amount");
        //register DataStream to Table
        tEnv.registerDataStream("tableB",orderB,"user,product,amount");

        Table result = tEnv.sqlQuery("select * from "+ tableA +" union all select * from tableB");

        tEnv.toAppendStream(result,Order.class).print();

        env.execute();

    }

    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}
