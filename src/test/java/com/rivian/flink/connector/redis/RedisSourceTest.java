/*
 * Copyright 2023 Rivian Automotive, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rivian.flink.connector.redis;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class RedisSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        RedisSubscriberSource source = new RedisSubscriberSource();
        source.setPatterns("test/*");
        DataStream<Message> stream = env.addSource(source);

        DataStream<Message> keyStream = stream.keyBy(Message::getMessage);

        PrintSinkFunction<Message> sink = new PrintSinkFunction<>();
        keyStream.addSink(sink).setParallelism(2);

        env.execute("RedisSourceTest");
    }
}
