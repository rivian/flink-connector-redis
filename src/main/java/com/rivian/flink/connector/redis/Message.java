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

public class Message {
  enum Type {
    MESSAGE, P_MESSAGE, SUBSCRIBE, UNSUBSCRIBE, P_SUBSCRIBE, P_UNSUBSCRIBE
  }

  Type type;
  String channel;
  String message;
  String pattern;
  int channels;

  public Message() {
  }

  public Message(Type type, String channel, String message, String pattern, int channels) {
    this.type = type;
    this.channel = channel;
    this.message = message;
    this.pattern = pattern;
    this.channels = channels;
  }

  public Type getType() {
    return type;
  }

  public String getChannel() {
    return channel;
  }

  public String getMessage() {
    return message;
  }

  public String getPattern() {
    return pattern;
  }

  public int getChannels() {
    return channels;
  }

  @Override
  public String toString() {
    return "Message{" +
        "type=" + type +
        ", channel='" + channel + '\'' +
        ", message='" + message + '\'' +
        ", pattern='" + pattern + '\'' +
        ", channels=" + channels +
        '}';
  }
}
