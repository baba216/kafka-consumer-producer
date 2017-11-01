package com.shubham.kafka.consumer;

import java.util.function.Consumer;

public class KafkaConsumer<T> {

  private Consumer<T> consumer;

  private String topic;


  public KafkaConsumer(Consumer<T> consumer) {
    this.consumer = consumer;

  }




}
