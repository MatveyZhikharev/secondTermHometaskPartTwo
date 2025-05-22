package org.example.service;

import static org.junit.jupiter.api.Assertions.*;

import org.example.dto.UserAuditDto;
import org.example.enums.Action;
import org.example.exception.UserNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@SpringBootTest
@Testcontainers
public class UserAuditServiceTest {
  @Container
  public static final CassandraContainer<?> cassandraContainer
      = new CassandraContainer<>("cassandra:3.11.2").withExposedPorts(9042);
  @Autowired
  private UserAuditService userService;

  @DynamicPropertySource
  static void cassandraProperties(DynamicPropertyRegistry registry) {
    String contactPoint =
        cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(9042);
    registry.add("spring.cassandra.contact-points", () -> contactPoint);
    registry.add("spring.cassandra.local-datacenter", () -> "datacenter1");
    registry.add("spring.cassandra.keyspace-name", () -> "my_keyspace");
  }

  @Test
  void testConnection() {
    assertTrue(cassandraContainer.isRunning());
  }

  @Test
  public void testCreateAndGetUserAuditById() {
    userService.insertUserAction(1L, Action.DROPPED_DATABASE, "Log1");

    List<UserAuditDto> result = userService.getUserAuditById(1L);

    assertFalse(result.isEmpty());
    assertEquals(1L, result.get(0).userId());
    assertEquals( Action.DROPPED_DATABASE.toString(), result.get(0).type());
    assertEquals("Log1", result.get(0).log());
  }

  @Test
  public void testGetUserAuditByIdNegative() {
    assertThrows(UserNotFoundException.class, () -> userService.getUserAuditById(2L));
  }
}