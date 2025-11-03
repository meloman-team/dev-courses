package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Репозиторий для работы с тикетами в базе данных YDB
 * Реализует операции добавления и чтения тикетов
 */
public class OperationYdbRepository {

    // Контекст для автоматических повторных попыток выполнения запросов
    // Принимается извне через конструктор для:
    // 1. Следования принципу Dependency Injection - зависимости класса передаются ему извне
    // 2. Улучшения тестируемости - можно передать mock-объект для тестов
    // 3. Централизованного управления конфигурацией ретраев
    // 4. Возможности переиспользования одного контекста для разных репозиториев
    private final QueryServiceHelper queryServiceHelper;

    public OperationYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    /**
     * Добавляет новую операцию в базу данных
     *
     * @param description описание операции
     * @return созданная операция со сгенерированным ID и временем создания
     */
    public Opetarion addOperation(String description) {
        // Генерируем случайный ID для тикета
        var id = ThreadLocalRandom.current().nextLong();
        var now = Instant.now();

        // Выполняем UPSERT запрос для добавления тикета
        // Изменять данные можно только в режиме транзакции SERIALIZABLE_RW, поэтому используем его
        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DECLARE $title AS Text;
                        DECLARE $created_at AS Timestamp;
                        UPSERT INTO t (operation_id, description, created_at)
                        VALUES ($id, $title, $created_at);
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of(
                        "$id", PrimitiveValue.newInt64(id),
                        "$title", PrimitiveValue.newText(description),
                        "$created_at", PrimitiveValue.newTimestamp(now)
                )
        );

        return new Opetarion(id, description, now);
    }

    /**
     * Добавляет новую операцию в базу данных без времени создания для вечного хранения записи без удаления по TTL
     *
     * @param description описание операции
     * @return созданная операция со сгенерированным ID без времени создания
     */
    public Opetarion addOperationTtlNull(String description) {
        // Генерируем случайный ID для тикета
        var id = ThreadLocalRandom.current().nextLong();

        // Выполняем UPSERT запрос для добавления тикета
        // Изменять данные можно только в режиме транзакции SERIALIZABLE_RW, поэтому используем его
        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DECLARE $title AS Text;
                        UPSERT INTO t (operation_id, description, created_at)
                        VALUES ($id, $title, CAST(null AS Timestamp?));
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of(
                        "$id", PrimitiveValue.newInt64(id),
                        "$title", PrimitiveValue.newText(description)
                )
        );

        return new Opetarion(id, description, null);
    }

}