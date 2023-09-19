package com.rp.reactorkafkaplayground.sec01;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/*
    goal: to demo a simple kafka consumer using reactor kafka
    producer -----> kafka broker <-----> consumer

    topic: order-events
    partitions: 1
    log-end-offset: 15 (15 events total)
    current-offset: 0 (no ACK) -> events need to be acknowledged for the current offset to be modified
    lag: 15

    if there is lag, when the consumer requests data, you get the lagged events
    (with no ACK, you get the same events again and again)

 */
@Slf4j
public class Lec02KafkaConsumerMultipleTopics {

    public static void main(String[] args) {

        var bootstrapsServerList = List.of("localhost:9092");
        var bootstrapServers = String.join(",", bootstrapsServerList);

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "inventory-service-group-01"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
