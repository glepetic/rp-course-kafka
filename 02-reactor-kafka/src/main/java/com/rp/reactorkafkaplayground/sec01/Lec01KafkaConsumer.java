package com.rp.reactorkafkaplayground.sec01;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
public class Lec01KafkaConsumer {

    public static void main(String[] args) {

        var bootstrapsServerList = List.of("localhost:9092");
        var bootstrapServers = String.join(",", bootstrapsServerList);

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, // "localhost:9092,localhost:9093,localhost:9098"
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, // key (for partition allocation)
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, // value (message content)
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-another", // consumer group
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // equivalent to --from-beginning
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "demo-group-another-instance-01", // instance of consumer in consumer group
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false // autocommits when true so that there is no need to manually acknowledge messages
        );

        var consumedTopics = List.of("order-events");

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(consumedTopics);

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
