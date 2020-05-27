package com.lpq.sql.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 读取file文件
 * readTextFile(path) - 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回。
 * readFile(fileInputFormat, path) - 根据指定的文件输入格式读取文件（一次）。
 * readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo) -
 * 这是上面两个方法内部调用的方法。它根据给定的 fileInputFormat 和读取路径读取文件。
 * 根据提供的 watchType，这个 source 可以
 * 定期（每隔 interval 毫秒）监测给定路径的新数据（FileProcessingMode.PROCESS_CONTINUOUSLY），
 * 或者处理一次路径对应文件的数据并退出（FileProcessingMode.PROCESS_ONCE）。
 * 你可以通过 pathFilter 进一步排除掉需要处理的文件。
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = FileSourceDemo.class.getClassLoader().getResource("words.txt").getPath();
        DataStream<String> words = env.readTextFile(filePath);

        words.print();

        env.execute("Flink read stream data from file!");
    }
}
