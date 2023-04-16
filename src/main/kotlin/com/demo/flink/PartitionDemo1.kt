package com.demo.flink

import com.demo.flink.entity.Person
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(
        Person("张三", 21),
        Person("李四", 16),
        Person("王老五", 35),
        Person("张三2", 22),
        Person("李四2", 17),
        Person("王老五2", 36)
    )

    dataSource.partitionCustom({ key, _ -> if (key < 20) 0 else if (key in 20 until 30) 1 else 2 }) { it.age }
        .print()

    env.execute("stream demo")
}