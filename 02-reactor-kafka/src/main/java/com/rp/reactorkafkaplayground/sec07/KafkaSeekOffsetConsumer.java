package com.rp.reactorkafkaplayground.sec07;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

/*
    goal: to seek offset
 */
@Slf4j
public class KafkaSeekOffsetConsumer {

    public static void main(String[] args) {

        var consumerConfig = KafkaUtil.getDefaultReceiverConfig("demo-group-another",
                "earliest",
                "demo-group-another-instance-01");

        var options = ReceiverOptions.create(consumerConfig)
                .addAssignListener(consumer -> {
                    consumer.forEach(r -> log.info("assigned {}", r.position()));
                    // start from current position - 2
                    consumer.forEach(r -> r.seek(r.position() - 2)); // seek parameter cannot be < 0
                    consumer.stream()
                            .filter(r -> r.topicPartition().partition() == 1) // find partition Number 2
                            .findFirst()
                            .ifPresent(r -> r.seek(r.position() - 2)); // start from current - 2 of partition number 1
                })
                .subscription(List.of("order-events"));

        var receiver = KafkaReceiver.create(options);

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
