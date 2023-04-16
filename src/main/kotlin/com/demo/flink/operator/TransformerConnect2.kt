package com.demo.flink.operator

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val ds1 = env.fromElements("good good study", "day day up")
    val ds2 = env.fromElements(1, 2, 3)

    val ds = ds1.connect(ds2)

    ds.flatMap(object : CoFlatMapFunction<String, Int, String> {
        override fun flatMap1(value: String, out: Collector<String>) {
            for (word in value.split(" ")) {
                out.collect(word)
            }
        }

        override fun flatMap2(value: Int, out: Collector<String>) {
            for (i in 1..value) {
                out.collect(i.toString())
            }
        }
    }).print()

    env.execute("flink connect transformation")
}