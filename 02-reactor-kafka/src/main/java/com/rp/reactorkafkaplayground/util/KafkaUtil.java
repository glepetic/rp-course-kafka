package com.rp.reactorkafkaplayground.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;

public class KafkaUtil {

    private static final List<String> BOOTSTRAP_SERVERS = List.of("localhost:9092");

    public static KafkaReceiver<Object, Object> getDefaultReceiver(String consumerGroup,
                                                                   String offsetReset,
                                                                   String consumerGroupInstance,
                                                                   List<String> consumedTopics) {

        var consumerConfig = getDefaultReceiverConfig(consumerGroup, offsetReset, consumerGroupInstance);

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(consumedTopics);

        return KafkaReceiver.create(options);
    }

    public static Map<String, Object> getDefaultReceiverConfig(String consumerGroup,
                                                               String offsetReset,
                                                               String consumerGroupInstance) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(), // "localhost:9092,localhost:9093,localhost:9098"
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, // key (for partition allocation)
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, // value (message content)
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroup, // consumer group
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset, // equivalent to --from-beginning when value is earliest
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, consumerGroupInstance, // instance of consumer in consumer group
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false // autocommits when true so that there is no need to manually acknowledge messages
        );
    }

    public static KafkaSender<String, String> getDefaultSender() {
        return KafkaSender.create(getDefaultSenderOptions());
    }

    // SenderOptions types define key and value types
    public static SenderOptions<String, String> getDefaultSenderOptions() {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return SenderOptions.create(producerConfig);
    }

    private static String getBootstrapServers() {
        return String.join(",", BOOTSTRAP_SERVERS);
    }

}
