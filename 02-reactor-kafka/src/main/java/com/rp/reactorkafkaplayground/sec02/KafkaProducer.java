package com.rp.reactorkafkaplayground.sec02;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;

@Slf4j
public class KafkaProducer {

    public static void main(String[] args) {

        var sender = KafkaUtil.getDefaultSender();

        sender
                .send(eventStream())
                .doOnNext(result -> log.info("Succesfully sent event with correlation id: {}", result.correlationMetadata()))
                .doOnComplete(sender::close) // no need if application runs on server mode and events are going to keep getting emitted
                .doOnError(err -> sender.close())
                .subscribe();

    }

    // SenderRecord<Key, Value, Result>
    private static Flux<SenderRecord<String, String, String>> eventStream() {
        return Flux.interval(Duration.ofMillis(100))
                .take(100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i)) // i is the key
                .map(pr -> SenderRecord.create(pr, pr.key()));
    }

}
