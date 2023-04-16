package com.demo.flink.operator

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import java.util.*

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val ds1 = env.fromElements("good good study")
        .map { it.lowercase(Locale.getDefault()) }
        .flatMap { value, out ->
            for (word in value.split(" ")) {
                out.collect(Tuple2.of(word, 1))
            }
        }
        .returns(Types.TUPLE(Types.STRING, Types.INT))

    val ds2 = env.fromElements("day day up")
        .map { it.lowercase(Locale.getDefault()) }
        .flatMap { value, out ->
            for (word in value.split(" ")) {
                out.collect(word)
            }
        }
        .returns(Types.STRING)

    val co = ds1.connect(ds2)

    co.process(object : CoProcessFunction<Tuple2<String, Int>, String, Tuple2<String, Int>>() {
        override fun processElement1(value: Tuple2<String, Int>, ctx: Context, out: Collector<Tuple2<String, Int>>) {
            out.collect(value)
        }

        override fun processElement2(value: String, ctx: Context, out: Collector<Tuple2<String, Int>>) {
            out.collect(Tuple2.of(value, 1))
        }
    }).print("process")

    co.map(object : CoMapFunction<Tuple2<String, Int>, String, Tuple2<String, Int>> {
        override fun map1(value: Tuple2<String, Int>): Tuple2<String, Int> {
            return Tuple2.of(value.f0.lowercase(Locale.getDefault()), value.f1)
        }

        override fun map2(value: String): Tuple2<String, Int> {
            return Tuple2.of(value, 1)
        }
    }).print("map")

    env.execute("flink connect transformation")
}