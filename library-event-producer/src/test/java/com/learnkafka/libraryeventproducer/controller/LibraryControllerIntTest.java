package com.learnkafka.libraryeventproducer.controller;

import com.learnkafka.libraryeventproducer.domain.Book;
import com.learnkafka.libraryeventproducer.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpStatus.CREATED;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"})
class LibraryControllerIntTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @Timeout(5)
    void putLibraryEvent() {
        var book = Book.builder()
                .bookName("Cloud Native Java")
                .bookAuthor("Josh Long")
                .bookId(1)
                .build();

        var libraryEvent = LibraryEvent
                .builder()
                .libraryEventId(123)
                .book(book)
                .build();

        testRestTemplate.put("/v1/libraryevent", libraryEvent, LibraryEvent.class);

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

        assertThat(consumerRecord.key()).isEqualTo(libraryEvent.getLibraryEventId());
        assertThat(consumerRecord.value()).isNotNull().isEqualTo("{\"libraryEventId\":123,\"libraryEventType\":\"UPDATED\",\"book\":{\"bookId\":1,\"bookName\":\"Cloud Native Java\",\"bookAuthor\":\"Josh Long\"}}");

    }

    @Test
    public void testAsyncPost_validateHttpStatusCreated() {

        var book = Book.builder()
                .bookName("Cloud Native Java")
                .bookAuthor("Josh Long")
                .bookId(1)
                .build();

        var libraryEvent = LibraryEvent
                .builder()
                .book(book)
                .build();

        var responseEntity = this.testRestTemplate
                .postForEntity("/v3/libraryevent", libraryEvent, LibraryEvent.class);

        assertThat(responseEntity.getStatusCode()).isEqualTo(CREATED);

        var consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

        assertThat(consumerRecord.key()).isNull();
        assertThat(consumerRecord.value()).isNotNull().isEqualTo("{\"libraryEventId\":null,\"libraryEventType\":\"CREATED\",\"book\":{\"bookId\":1,\"bookName\":\"Cloud Native Java\",\"bookAuthor\":\"Josh Long\"}}");
    }

}
