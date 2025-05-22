package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Application;
import org.example.config.ApplicationConfig;
import org.example.config.KafkaTopicConfig;
import org.example.dto.UserAuditDto;
import org.example.enums.Action;
import org.example.exception.UserNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest(
    classes = {
        KafkaConsumerService.class,
        ApplicationConfig.class,
        Application.class,
        KafkaTopicConfig.class,
        KafkaAutoConfiguration.class},
    properties = {
        "topic-to-consume-message=audit",
        "spring.kafka.consumer.group-id=audit-group",
        "spring.kafka.consumer.auto-offset-reset=earliest"
    }
)
@Import({KafkaAutoConfiguration.class})
@Testcontainers
class KafkaConsumerServiceTest {
  @Container
  @ServiceConnection
  public static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
  @Container
  private static final CassandraContainer<?> cassandraContainer =
      new CassandraContainer<>("cassandra:3.11.2")
          .withExposedPorts(9042);
  @Autowired
  private UserAuditService userAuditService;
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private ObjectMapper objectMapper;

  @DynamicPropertySource
  static void cassandraProperties(DynamicPropertyRegistry registry) {
    String contactPoint =
        cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(9042);
    registry.add("spring.cassandra.contact-points", () -> contactPoint);
    registry.add("spring.cassandra.local-datacenter", () -> "datacenter1");
    registry.add("spring.cassandra.keyspace-name", () -> "my_keyspace");
  }

  @Test
  void shouldSendMessageToKafkaPositive() throws JsonProcessingException {
    UserAuditDto userAudit = new UserAuditDto(1L, Instant.now(), Action.INSERT.toString(), "test");
    kafkaTemplate.send("audit", objectMapper.writeValueAsString(userAudit));

    await().atMost(Duration.ofSeconds(10))
        .pollDelay(Duration.ofMillis(500))
        .untilAsserted(() -> {
              UserAuditDto recievedUserAudit = userAuditService.getUserAuditById(1L).get(0);
              assertEquals(userAudit.userId(), recievedUserAudit.userId());
              assertEquals(userAudit.type(), recievedUserAudit.type());
              assertTrue(userAudit.timestamp().isBefore(recievedUserAudit.timestamp()));
              assertEquals(userAudit.log(), recievedUserAudit.log());
            }
        );
  }

  @Test
  void shouldSendMessageToKafkaNegative() throws JsonProcessingException {
    UserAuditDto userAudit = new UserAuditDto(1L, Instant.now(), Action.INSERT.toString(), "test");
    kafkaTemplate.send("audit", objectMapper.writeValueAsString(userAudit));

    await().atMost(Duration.ofSeconds(10))
        .pollDelay(Duration.ofMillis(500))
        .untilAsserted(() -> {
              assertThrows(UserNotFoundException.class, () -> userAuditService.getUserAuditById(2L).isEmpty());
            }
        );
  }
}