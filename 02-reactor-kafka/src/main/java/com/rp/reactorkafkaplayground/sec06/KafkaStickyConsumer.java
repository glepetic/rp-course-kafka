package com.rp.reactorkafkaplayground.sec06;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
    goal: to demo partition re-balancing. Ensure that topic has multiple partitions

 */
@Slf4j
public class KafkaStickyConsumer {

    public static void start(String instanceId) {

        var consumerConfig = KafkaUtil.getDefaultReceiverConfig(
                "demo-group-123",
                "earliest",
                instanceId
        );

        var customConfig = consumerConfig.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // default is RangeAssignor
        customConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        var receiver = KafkaReceiver.create(options);

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }

}
