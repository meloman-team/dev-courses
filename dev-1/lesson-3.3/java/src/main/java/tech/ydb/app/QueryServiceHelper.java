package tech.ydb.app;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;

/**
 * @author Kirill Kurdyukov
 */
public class QueryServiceHelper {

    // Контекст для автоматических повторных попыток выполнения запросов
    // Принимается извне через конструктор для:
    // 1. Следования принципу Dependency Injection - зависимости класса передаются ему извне
    // 2. Улучшения тестируемости - можно передать mock-объект для тестов
    // 3. Централизованного управления конфигурацией ретраев
    // 4. Возможности переиспользования одного контекста для разных репозиториев
    private final SessionRetryContext retryCtx;

    public QueryServiceHelper(SessionRetryContext retryCtx) {
        this.retryCtx = retryCtx;
    }

    public void executeQuery(String yql) {
        retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(yql, TxMode.NONE))
        ).join().getValue();
    }

    public QueryReader executeQuery(String yql, TxMode txMode, Params params) {
        return retryCtx.supplyResult(
                session -> QueryReader.readFrom(session.createQuery(yql, txMode, params))
        ).join().getValue();
    }
}
