package org.example.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.example.cassandra.CassandraConfig;
import org.example.dto.UserAuditDto;
import org.example.enums.Action;
import org.example.exception.UserNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class UserAuditService {
  @Autowired
  private CassandraConfig cassandraConfig;
  @Autowired
  private CqlSession session;

  public void insertUserAction(UUID userId, Action type, String log) {
    PreparedStatement preparedStatement = session.prepare(
        "INSERT INTO my_keyspace.user_audits (user_id, timestamp, type, log) " +
            "VALUES (?, ?, ?, ?)"
    );

    BoundStatement boundStatement = preparedStatement.bind(
        userId,
        java.time.Instant.now(),
        type.toString(),
        log
    );

    session.execute(boundStatement);
  }

  public List<UserAuditDto> getUserAuditById(UUID userId) throws UserNotFoundException {
    PreparedStatement statement =
        session.prepare(
            "SELECT * FROM "
                + "my_keyspace.user_audits WHERE user_id = ?");
    BoundStatement boundStatement = statement.bind(userId);

    ResultSet rows = session.execute(boundStatement);

    if (!rows.iterator().hasNext()) {
      throw new UserNotFoundException("User with UUID " + userId + " wasn't found");
    }

    List<UserAuditDto> userAudits = new ArrayList<>();
    for (Row row : rows) {
      userAudits.add(
          new UserAuditDto(
              row.getUuid("user_id"),
              row.getInstant("timestamp"),
              row.getString("type"),
              row.getString("log")
          )
      );
    }
    return userAudits;
  }
}
