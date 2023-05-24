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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import redis.clients.jedis.*;

public class RedisSubscriberSource extends RichParallelSourceFunction<Message> {

    transient UnifiedJedis jedis;
    transient Subscriber subscriber;

    String host = Protocol.DEFAULT_HOST;
    int port = Protocol.DEFAULT_PORT;
    int timeout = Protocol.DEFAULT_TIMEOUT;
    int database = Protocol.DEFAULT_DATABASE;
    String clientName;
    String user;
    String password;
    boolean useSSL;

    Mode mode = Mode.STANDALONE;
    String[] channels;
    String[] patterns;

    boolean emitControl;

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxIdle(1);
        if (mode == Mode.STANDALONE) {
            jedis = new JedisPooled(poolConfig, host, port, timeout, user, password, database, clientName, useSSL);
        } else {
            HostAndPort node = new HostAndPort(host, port);
            jedis = new JedisCluster(node, timeout, timeout, JedisCluster.DEFAULT_MAX_ATTEMPTS, user, password, clientName, poolConfig, useSSL);
        }
    }

    @Override
    public void run(SourceContext<Message> ctx) throws Exception {
        subscriber = new Subscriber(ctx);

        if (patterns != null) {
            jedis.psubscribe(subscriber, patterns);
        } else {
            jedis.subscribe(subscriber, channels);
        }
    }

    @Override
    public void cancel() {
        subscriber.unsubscribe();
        jedis.close();
    }

    private class Subscriber extends JedisPubSub {

        SourceContext<Message> ctx;

        Subscriber(SourceContext<Message> ctx) {
            this.ctx = ctx;
        }

        @Override
        public void onMessage(String channel, String message) {
            ctx.collect(new Message(Message.Type.MESSAGE, channel, message, null, 0));
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
            ctx.collect(new Message(Message.Type.P_MESSAGE, channel, message, pattern, 0));
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (emitControl) {
                ctx.collect(new Message(Message.Type.SUBSCRIBE, channel, null, null, subscribedChannels));
            }
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            if (emitControl) {
                ctx.collect(new Message(Message.Type.UNSUBSCRIBE, channel, null, null, subscribedChannels));
            }
        }

        @Override
        public void onPUnsubscribe(String pattern, int subscribedChannels) {
            if (emitControl) {
                ctx.collect(new Message(Message.Type.P_UNSUBSCRIBE, null, null, pattern, subscribedChannels));
            }
        }

        @Override
        public void onPSubscribe(String pattern, int subscribedChannels) {
            if (emitControl) {
                ctx.collect(new Message(Message.Type.P_SUBSCRIBE, null, null, pattern, subscribedChannels));
            }
        }
    }

    public enum Mode {
        STANDALONE, CLUSTER
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public String[] getChannels() {
        return channels;
    }

    public void setChannels(String... channels) {
        this.channels = channels;
    }

    public String[] getPatterns() {
        return patterns;
    }

    public void setPatterns(String... patterns) {
        this.patterns = patterns;
    }

    public boolean isEmitControl() {
        return emitControl;
    }

    public void setEmitControl(boolean emitControl) {
        this.emitControl = emitControl;
    }
}
