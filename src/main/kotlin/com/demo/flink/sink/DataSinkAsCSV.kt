package com.demo.flink.sink

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import java.util.*

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.fromElements("Good good study", "Day day up")
        .map { it.lowercase(Locale.getDefault()) }
        .flatMap { value, out ->
            for (word in value.split(" ")) {
                out.collect(Tuple2.of(word, 1))
            } }
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .writeAsCsv("result.csv")
        .setParallelism(1)

    env.execute("Data Sink Demo")
}