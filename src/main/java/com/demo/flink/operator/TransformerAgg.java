package com.demo.flink.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class TransformerAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStreamSource = env.fromElements(new Tuple2<>("good", 1), new Tuple2<>("good", 2), new Tuple2<>("study", 1));

        dataStreamSource.keyBy(t -> t.f0).sum(1).print();

        env.execute("flink aggregation transformation");
    }
}
