package com.demo.flink.partition

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.util.*

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5)

    val stream = dataSource.map { Tuple2.of(it % 2, it) }.returns(Types.TUPLE(Types.INT, Types.INT))
        .keyBy { it.f0 }
        .map { Tuple2.of(it.f0, it.f1) }.returns(Types.TUPLE(Types.INT, Types.INT))
        .setParallelism(2)

    println(stream.parallelism)

    stream.shuffle().print("shuffle").setParallelism(3)

    env.execute("shuffle分区实例")
}