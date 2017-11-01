package com.shubham.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaProducerExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerExample.class);

  private final static String TOPIC = "my-example-topic";
  private final static String BOOTSTRAP_SERVERS =
      "localhost:9092";

  private static Producer<Long,String> createProducer(){
    Properties properties = new Properties();
    //Above KafkaProducerExample.createProducer sets the BOOTSTRAP_SERVERS_CONFIG (“bootstrap.servers) property
    // to the list of broker addresses we defined earlier. BOOTSTRAP_SERVERS_CONFIG value is a comma separated list
    // of host/port pairs that the Producer uses to establish an initial connection to the Kafka cluster.
    // The producer uses of all servers in the cluster no matter which ones we list here.
    // This list only specifies the initial Kafka brokers used to discover the full set of servers of the Kafka cluster.
    // If a server in this list is down, the producer will just go to the next broker in the list to discover the full
    // topology of the Kafka cluster.
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
    //The CLIENT_ID_CONFIG (“client.id”) is an id to pass to the server when making requests so the server can track
    // the source of requests beyond just IP/port by passing a producer name for things like server-side request logging.
    properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaExampleProducer");
    //The KEY_SERIALIZER_CLASS_CONFIG (“key.serializer”) is a Kafka Serializer class for Kafka record keys that implements
    // the Kafka Serializer interface. Notice that we set this to LongSerializer as the message ids in our example are longs.
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    //The VALUE_SERIALIZER_CLASS_CONFIG (“value.serializer”) is a Kafka Serializer class for Kafka record values that implements
    // the Kafka Serializer interface. Notice that we set this to StringSerializer as the message body in our example are strings.
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(properties);
  }

  static void runProducer(final int sendMessageCount) throws Exception {
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();
    try {
      for (long index = time; index < time + sendMessageCount; index++) {
        final ProducerRecord<Long, String> record =
            new ProducerRecord<>(TOPIC, index, "Hello" + index);
        Future<RecordMetadata>  future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        long elapsedTime = System.currentTimeMillis();
        LOGGER.debug("===:::== Sent Record(Key={},Value={})::meta(partition={},offset={})::time:{} ===::==",
            record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(),
            elapsedTime);
      }
    } finally {
      producer.flush();
      producer.close();
    }
  }

  static void runProducerAsync(final int sendMessageCount) throws Exception {
    final Producer<Long, String> producer = createProducer();
    long time = System.currentTimeMillis();
    final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
    try {
      for (long index = time; index < time + sendMessageCount; index++) {
        final ProducerRecord<Long, String> record =
            new ProducerRecord<>(TOPIC, index, "Hello" + index);
        producer.send(record, ((recordMetadata, exception) -> {
          long elapsedTime = System.currentTimeMillis();
          if (recordMetadata != null) {
            LOGGER.debug("Sent Record(Key={},Value={})::meta(partition={},offset={})::time:{}",
                record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset(),
                elapsedTime);
          } else {
            exception.printStackTrace();
          }
          countDownLatch.countDown();
        }));
      }
      countDownLatch.await(25, TimeUnit.SECONDS);
    } finally {
      producer.flush();
      producer.close();
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Going to start producer");
    runProducer(3);
  }
}
