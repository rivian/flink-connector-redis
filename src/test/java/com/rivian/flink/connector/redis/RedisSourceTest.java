package com.rivian.flink.connector.redis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 5/12/22
 */
public class RedisSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        RedisSubscriberSource source = new RedisSubscriberSource();
        source.setPatterns("test/*");
        DataStream<RedisSubscriberSource.Message> stream = env.addSource(source);

        DataStream<RedisSubscriberSource.Message> keyStream = stream.keyBy(RedisSubscriberSource.Message::getMessage);

        PrintSinkFunction<RedisSubscriberSource.Message> sink = new PrintSinkFunction<>();
        keyStream.addSink(sink).setParallelism(2);

        env.execute("RedisSourceTest");
    }
}
