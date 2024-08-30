package org.example.producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.producer.model.Transaction;
import org.example.producer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api")
public class TransactionController {

    private final ProducerService producerService;

    @Autowired
    public TransactionController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/transactions")
    public ResponseEntity<String> createTransaction(@RequestBody Transaction transaction) {
        try {
            producerService.publishTransaction(transaction);
            return new ResponseEntity<>("Transaction created", HttpStatus.CREATED);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            return new ResponseEntity<>("Failed to create transaction", HttpStatus.BAD_REQUEST);
        }
    }
}