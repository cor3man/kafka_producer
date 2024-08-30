package org.example.producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.producer.model.Client;
import org.example.producer.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api")
public class ClientController {

    private final ProducerService producerService;

    @Autowired
    public ClientController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/clients")
    public ResponseEntity<String> createClient(@RequestBody Client client) {
        try {
            producerService.publishClient(client);
            return new ResponseEntity<>("Client created", HttpStatus.CREATED);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
            return new ResponseEntity<>("Failed to create client", HttpStatus.BAD_REQUEST);
        }
    }
}