package com.rp.reactorkafkaplayground.sec01;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

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

        var receiver = KafkaUtil.getDefaultReceiver("demo-group-another",
                "earliest",
                "demo-group-another-instance-01",
                List.of("order-events"));

        receiver
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge()) // has commit interval, acknowledges periodically
                .subscribe();

    }

}
