package com.rp.reactorkafkaplayground.sec04;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.function.Function;

@Slf4j
public class KafkaWithHeadersProducer {

    public static void main(String[] args) {

        var sender = KafkaUtil.getDefaultSender();

        sender
                .send(eventStream())
                .doOnNext(result -> log.info("Succesfully sent event with correlation id: {}", result.correlationMetadata()))
                .doOnComplete(sender::close)
                .doOnError(err -> sender.close())
                .subscribe();

    }

    private static Flux<SenderRecord<String, String, String>> eventStream() {
        Function<Integer, Headers> buildHeaders = i -> new RecordHeaders()
                .add("client-id", "some-client".getBytes())
                .add("tracing-id", "123456-".concat(i.toString()).getBytes());
        return Flux.range(1, 100)
                .map(i -> new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, buildHeaders.apply(i)))
                .map(pr -> SenderRecord.create(pr, pr.key()));
    }

}
