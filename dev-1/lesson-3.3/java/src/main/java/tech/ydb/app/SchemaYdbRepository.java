package tech.ydb.app;

import tech.ydb.query.tools.SessionRetryContext;

/**
 * Репозиторий для управления схемой базы данных YDB
 * Отвечает за создание и удаление таблиц
 */
public class SchemaYdbRepository {

    private final QueryServiceHelper queryServiceHelper;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    /**
     * Создает таблицу t в базе данных
     * с настройкой автоудаления старых данных
     */
    public void createSchema() {
        queryServiceHelper.executeQuery("""
                CREATE TABLE t (
                    operation_id Int64 NOT NULL,
                    description Text,
                    created_at Timestamp?,
                    PRIMARY KEY (operation_id)
                ) WITH (
                    TTL = Interval("PT15M") ON created_at -- строки будут удаляться спустя 15 мин после наступления времени, записанного в колонке created_at:
                )
                """
        );
    }

    /**
     * Настройка автоматического разбиения на разделы
     */
    public void configureAutoPartitioning() {
        queryServiceHelper.executeQuery("""
                ALTER TABLE t SET (
                  AUTO_PARTITIONING_BY_LOAD=ENABLED, -- включение автопартиционирования по нагрузке
                  AUTO_PARTITIONING_BY_SIZE=ENABLED, -- включение партиционирования по размеру
                  AUTO_PARTITIONING_PARTITION_SIZE_MB=1 -- максимальный размер шарда
                )
                """
        );
    }

    /**
     * Удаляет таблицу issues из базы данных
     * Используется для очистки схемы перед созданием новой
     */
    public void dropSchema() {
        queryServiceHelper.executeQuery("DROP TABLE IF EXISTS t;");
    }
}
