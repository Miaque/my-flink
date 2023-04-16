package com.demo.flink.partition

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements("1", "2", "3", "4", "5")

    val stream = dataSource.map { Tuple2.of((it.toInt() % 2).toString(), it) }
        .returns(Types.TUPLE(Types.STRING, Types.STRING))

    stream.partitionCustom({ key, _ -> key.toInt() % 2 }) {
        it.f0
    }.print().setParallelism(2)

    env.execute("自定义分区示例")
}