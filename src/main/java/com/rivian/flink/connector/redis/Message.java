package com.rivian.flink.connector.redis;

/**
 * Created by Pramod Immaneni <pimmaneni@rivian.com> on 5/24/23
 */
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
