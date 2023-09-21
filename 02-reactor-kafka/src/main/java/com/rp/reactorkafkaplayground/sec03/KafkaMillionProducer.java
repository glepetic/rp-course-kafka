package com.rp.reactorkafkaplayground.sec03;

import com.rp.reactorkafkaplayground.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Slf4j
public class KafkaMillionProducer {

    public static void main(String[] args) {

        var start = System.currentTimeMillis();
        var senderOptions = KafkaUtil.getDefaultSenderOptions()
                // prefetch size increased from 256 to 10K, emission is faster, but watch out for subscriber processing speed
                .maxInFlight(10_000);
        var sender = KafkaSender.create(senderOptions);

        sender
                .send(eventStream())
                .doOnNext(result -> log.info("Succesfully sent event with correlation id: {}", result.correlationMetadata()))
                .doOnComplete(() -> log.info("Total time taken: {} ms", (System.currentTimeMillis() - start)))
                .subscribe();

    }

    private static Flux<SenderRecord<String, String, String>> eventStream() {
        return Flux.range(1, 1_000_000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));
    }

}
