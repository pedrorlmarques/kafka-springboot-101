package com.learnkafka.libraryeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventproducer.component.LibraryEventProducer;
import com.learnkafka.libraryeventproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventproducer.domain.LibraryEventType;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Log4j2
public class LibraryController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryAsync(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Before sendLibraryEvent");
        libraryEvent.setLibraryEventType(LibraryEventType.CREATED);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/v2/libraryevent")
    public ResponseEntity<LibraryEvent> createLibrarySync(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Before sendLibraryEvent");
        libraryEvent.setLibraryEventType(LibraryEventType.CREATED);
        libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }


    @PostMapping("/v3/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryAsyncSend(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("Before sendLibraryEvent");
        libraryEvent.setLibraryEventType(LibraryEventType.CREATED);
        libraryEventProducer.sendLibraryEventSend(libraryEvent);
        log.info("After sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {


        if (libraryEvent.getLibraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATED);
        libraryEventProducer.sendLibraryEventSend(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
