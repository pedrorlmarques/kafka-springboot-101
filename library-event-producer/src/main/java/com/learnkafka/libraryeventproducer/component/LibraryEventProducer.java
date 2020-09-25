package com.learnkafka.libraryeventproducer.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventproducer.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Log4j2
@Component
@RequiredArgsConstructor
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var result = this.kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
                objectMapper.writeValueAsString(libraryEvent));

        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(result.getProducerRecord().key(), result.getProducerRecord().value(), result);
            }
        });

    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventSend(LibraryEvent libraryEvent) throws JsonProcessingException {

        var producerRecord = buildProducerRecord(libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent), "library-events");

        var result = this.kafkaTemplate.send(producerRecord);

        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(result.getProducerRecord().key(), result.getProducerRecord().value(), result);
            }
        });

        return result;

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<Integer, String>(topic, null, key, value, headers);
    }

    public void sendLibraryEventSynchronous(LibraryEvent libraryEvent) {

        try {
            var result = this.kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),
                    objectMapper.writeValueAsString(libraryEvent))
                    .get(1, TimeUnit.SECONDS);
            handleSuccess(result.getProducerRecord().key(), result.getProducerRecord().value(), result);
        } catch (Exception e) {
            handleFailure(e);
        }

    }

    private void handleFailure(Throwable ex) {
        log.error("Error Sending the message {} ", ex.getMessage());
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

        log.info("Message Sent Successfully for the key {} and value {}, partition {} ", key, value,
                result.getRecordMetadata().partition());
    }
}
