package tech.ydb.app;

import java.time.Instant;

/**
 * Модель данных для представления тикета в примере
 */
public record Opetarion(
        long operationId,   // Уникальный идентификатор операции
        String description, // Описание операции
        Instant createdAt   // Время создания тикета
) {
}