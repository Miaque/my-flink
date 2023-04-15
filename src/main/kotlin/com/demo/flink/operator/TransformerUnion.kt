package com.demo.flink.operator

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.util.*

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val ds1 = env.fromElements("good good study")
        .map { it.lowercase(Locale.getDefault()) }
        .flatMap { value, out ->
            run {
                for (word in value.split(" ")) {
                    out.collect(word)
                }
            }
        }
        .returns(Types.STRING)
        .map { Tuple2.of(it, 1) }
        .returns(Types.TUPLE(Types.STRING, Types.INT))

    val ds2 = env.fromElements("day day up")
        .map { it.lowercase(Locale.getDefault()) }
        .flatMap { value, out ->
            run {
                for (word in value.split(" ")) {
                    out.collect(word)
                }
            }
        }
        .returns(Types.STRING)
        .map { Tuple2.of(it, 1) }
        .returns(Types.TUPLE(Types.STRING, Types.INT))

    ds1.union(ds2)
        .print()

    env.execute("flink union transformation")
}