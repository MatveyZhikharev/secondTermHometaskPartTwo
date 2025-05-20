package org.example.dto;

import java.time.Instant;
import java.util.UUID;

public record UserAuditDto(UUID userId, Instant timestamp, String type, String log) {
};
