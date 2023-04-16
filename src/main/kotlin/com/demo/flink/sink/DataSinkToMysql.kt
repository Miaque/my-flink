package com.demo.flink.sink

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.enableCheckpointing(TimeUnit.SECONDS.toMillis(2))

    env.fromElements("Good good study", "Day day up")
        .map { it.lowercase() }
        .flatMap { value, out ->
            for (word in value.split(" ")) {
                out.collect(Tuple2.of(word, 1))
            }
        }
        .returns(Types.TUPLE(Types.STRING, Types.INT))
        .addSink(DataSinkToMysql())

    env.execute("Data Sink Demo")
}

class DataSinkToMysql : RichSinkFunction<Tuple2<String, Int>>() {

    companion object {
        const val JDBC_URL = "jdbc:mysql://localhost:3306/flink?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=GMT%2B8&nullCatalogMeansCurrent=true&allowPublicKeyRetrieval=true"
        const val DRIVER = "com.mysql.cj.jdbc.Driver"
        const val USER_NAME = "root"
        const val USER_PWD = "root"
    }

    private lateinit var stmt: PreparedStatement
    private lateinit var conn: Connection

    override fun open(parameters: Configuration) {
        Class.forName(DRIVER)
        conn = DriverManager.getConnection(JDBC_URL, USER_NAME, USER_PWD)
        val sql = "insert into wc(word, cnt) value (?, ?)"
        stmt = conn.prepareStatement(sql)
    }

    override fun close() {
        super.close()
        stmt?.close()
        conn?.close()
    }

    override fun invoke(value: Tuple2<String, Int>, context: SinkFunction.Context) {
        stmt.setString(1, value.f0)
        stmt.setInt(2, value.f1)
        stmt.executeUpdate()
    }

}