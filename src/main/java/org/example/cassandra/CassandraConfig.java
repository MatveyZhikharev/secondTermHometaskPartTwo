package org.example.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.util.Map;

@Configuration
public class CassandraConfig {

  @Bean
  public CqlSession cqlSession(CqlSessionBuilder sessionBuilder) {
    InetSocketAddress address = InetSocketAddress.createUnresolved("127.0.0.1", 9042);
    sessionBuilder = sessionBuilder.addContactPoint(address);
    sessionBuilder.withKeyspace((CqlIdentifier) null);

    CqlSession session = sessionBuilder.build();

    SimpleStatement statement = SchemaBuilder.createKeyspace("my_keyspace")
        .ifNotExists()
        .withNetworkTopologyStrategy(Map.of("datacenter1", 1))
        .build();
    session.execute(statement);

    session.execute("""
            CREATE TABLE IF NOT EXISTS my_keyspace.user_audits (
                user_id UUID,
                timestamp TIMESTAMP,
                type TEXT,
                log TEXT,
                PRIMARY KEY ((user_id), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
               AND default_time_to_live = 31536000;
            """);

    return sessionBuilder
        .withKeyspace("my_keyspace")
        .build();
  }
}