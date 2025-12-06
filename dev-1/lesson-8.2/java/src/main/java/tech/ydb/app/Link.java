package tech.ydb.app;

/**
 * Модель данных для представления записи в таблице links
 */
public record Link(
        long source,        // Идентификатор тикета
        long destination  // Идентификатор связанного тикета
) {
}
