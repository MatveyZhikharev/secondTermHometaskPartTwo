package org.example.dto;

import java.time.Instant;

public record UserAuditDto(Long userId, Instant timestamp, String type, String log) {
};
