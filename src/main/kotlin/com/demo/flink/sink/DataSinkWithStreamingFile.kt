package com.demo.flink.sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(60_000)

    val sink = StreamingFileSink.forRowFormat(Path("./result"), SimpleStringEncoder<Tuple2<String, Int>>())
        .build()

    val dataSource = env.fromElements("Good good study", "Day day up")
        .map { it.lowercase() }
        .flatMap { value, out ->
            for (word in value.split(" ")) {
                out.collect(Tuple2.of(word, 1))
            }
        }
        .returns(Types.TUPLE(Types.STRING, Types.INT))

    dataSource.addSink(sink).name("Flink2File").setParallelism(1)

    env.execute("Data Sink Demo")
}