package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.UserAuditDto;
import org.example.enums.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListener.class);

  private UserAuditService userAuditService;
  private ObjectMapper objectMapper;

  public KafkaConsumerService(UserAuditService userAuditService, ObjectMapper objectMapper) {
    this.userAuditService = userAuditService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = {"${topic-to-send-message}"})
  public void consumeMessage(String message) throws JsonProcessingException {
    LOGGER.info("Retrieved message {}", message);
    UserAuditDto parsedMessage = objectMapper.readValue(message, UserAuditDto.class);
    userAuditService.insertUserAction(parsedMessage.userId(), Action.valueOf(parsedMessage.type()), parsedMessage.log());
  }
}
