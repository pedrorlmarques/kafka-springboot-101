package com.learnkafka.libraryeventproducer.domain;


import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Book {

    Integer bookId;
    String bookName;
    String bookAuthor;
}
