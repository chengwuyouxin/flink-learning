package com.lpq.sql.source;

/**
 * 自定义Source需要实现SourceFunction接口
 * SourceFunction 定义了两个接口方法：
 * 1、run ： 启动一个 source，即对接一个外部数据源然后 emit 元素形成 stream（大部分情况下会通过在该方法里运行一个 while 循环的形式来产生 stream）。
 * 2、cancel ： 取消一个 source，也即将 run 中的循环 emit 元素的行为终止。
 */
public class SelfDefSourceDemo {
    public static void main(String[] args) {

    }
}
