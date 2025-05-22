package org.example.repository;

import org.example.entity.UserAudit;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface UserAuditRepository extends CassandraRepository<UserAudit, String> {
  @Query("SELECT * FROM user_audits WHERE user_id = ?0")
  UserAudit findByUserId(Long userId);
}
