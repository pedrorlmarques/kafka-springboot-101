package com.learnkafka.libraryeventproducer.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {

    Integer libraryEventId;
    LibraryEventType libraryEventType;
    Book book;

}
