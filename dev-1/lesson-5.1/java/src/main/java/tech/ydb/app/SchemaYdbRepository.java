package tech.ydb.app;

import tech.ydb.query.tools.SessionRetryContext;

/**
 * Репозиторий для управления схемой базы данных YDB
 * Отвечает за создание и удаление таблиц и индексов
 *
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    private final QueryServiceHelper queryServiceHelper;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    /**
     * Создает таблицы и индексы в базе данных
     */
    public void createSchema() {
        queryServiceHelper.executeQuery("""
                 CREATE TABLE IF NOT EXISTS issues (
                     id Int64 NOT NULL,
                     title Text NOT NULL,
                     created_at Timestamp NOT NULL,
                     author Text,
                     PRIMARY KEY (id)
                 );
                """
        );

        // Создаем глобальный вторичный индекс по полю author
        // Этот индекс позволяет эффективно искать тикеты по автору
        queryServiceHelper.executeQuery("ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);");

        // Добавляем колонку link_count и создаем таблицу для связей между тикетами
        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD COLUMN link_count Int64;
                CREATE TABLE links (
                    source Int64 NOT NULL,
                    destination Int64 NOT NULL,
                    PRIMARY KEY(source, destination)
                );
                """
        );

        // Создаем глобальный вторичный индекс по полю author и created_at
        // Этот индекс позволяет эффективно искать тикеты по автору через '=' и условию по created_at например '>'
        queryServiceHelper.executeQuery("ALTER TABLE issues ADD INDEX authorAndCreatedAtIndex GLOBAL ON (author, created_at) COVER (title, link_count);");
    }

    public void dropSchema() {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS issues;
                DROP TABLE IF EXISTS links;
                """
        );
    }
}
