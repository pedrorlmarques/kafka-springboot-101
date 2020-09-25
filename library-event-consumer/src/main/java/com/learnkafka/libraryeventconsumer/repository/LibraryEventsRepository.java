package com.learnkafka.libraryeventconsumer.repository;

import com.learnkafka.libraryeventconsumer.domain.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventsRepository extends JpaRepository<LibraryEvent, Integer> {
}
