package tech.ydb.app;

import tech.ydb.core.Result;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.settings.ReadRowsSettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Kirill Kurdyukov
 */
public class KeyValueApiYdbRepository {

    private final SessionRetryContext retryTableCtx;

    //Названия колонок в таблице issues
    private final String idColumnName = "id";
    private final String titleColumnName = "title";
    private final String createdAtColumnName = "created_at";
    private final String authorColumnName = "author";
    private final String linkCountColumnName = "link_count";
    private final String statusColumnName = "status";

    // Названия колонок в таблице links
    private final String sourceColumnName = "source";
    private final String destinationColumnName = "destination";

    public KeyValueApiYdbRepository(SessionRetryContext retryTableCtx) {
        this.retryTableCtx = retryTableCtx;
    }

    /**
     * Массовое добавление или обновление тикетов в таблице.
     */
    public void bulkUpsert(String tableName, List<Issue> issues) {

        // Описывает структуру с полями, которые будут добавляться в таблицу.
        // Смысл операции тот же что для запроса UPSERT. Поля первичного ключа - обязательные, 
        // остальные - опциональные. Если запись с таким первичным ключём уже существует, то
        // переданные поля обновятся, а остальные - сохранят прежние значения.
        var structType = StructType.of(Map.of(
                idColumnName, PrimitiveType.Int64,
                titleColumnName, PrimitiveType.Text,
                createdAtColumnName, PrimitiveType.Timestamp,
                authorColumnName, PrimitiveType.Text,
                linkCountColumnName, PrimitiveType.Int64,
                statusColumnName, PrimitiveType.Text
        ));

        var listIssues = ListType.of(structType).newValue(
                issues.stream().map(issue -> structType.newValue(Map.of(
                        idColumnName, PrimitiveValue.newInt64(issue.id()),
                        titleColumnName, PrimitiveValue.newText(issue.title()),
                        createdAtColumnName, PrimitiveValue.newTimestamp(issue.now()),
                        authorColumnName, PrimitiveValue.newText(issue.author()),
                        linkCountColumnName, PrimitiveValue.newInt64(issue.linkCounts()),
                        statusColumnName, PrimitiveValue.newText(issue.status())
                ))).toList()
        );

        retryTableCtx.supplyStatus(session -> session.executeBulkUpsert(tableName, listIssues))
                .join().expectSuccess();
    }

    /**
     * Массовое обновление связей в таблице links
     */
    public void bulkUpsertLinks(List<Link> links) {
        var structType = StructType.of(
                sourceColumnName, PrimitiveType.Int64,
                destinationColumnName, PrimitiveType.Int64
        );
        var listLinks = ListType.of(structType).newValue(
                links.stream().map(link -> structType.newValue(
                        sourceColumnName, PrimitiveValue.newInt64(link.source()),
                        destinationColumnName, PrimitiveValue.newInt64(link.destination())
                )).toList()
        );
        retryTableCtx.supplyStatus(session -> session.executeBulkUpsert("/local/links", listLinks))
                .join().expectSuccess();
    }

    /**
     * Чтение всех данных из таблицы.
     * Использует executeReadTable для получения всех записей.
     */
    public List<Issue> readTable(String tableName) {
        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    session.executeReadTable(tableName, ReadTableSettings.newBuilder().build())
                            .start(
                                    readTablePart -> {
                                        var resultSetReader = readTablePart.getResultSetReader();

                                        fetchIssues(listResult, resultSetReader);
                                    }
                            ).join().expectSuccess();


                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    /**
     * Подсчет всех записей в таблице у которых есть хотя бы одна связь
     */
    public AtomicLong readTableCountLink() {
        return retryTableCtx.supplyResult(session -> {
                    AtomicLong count = new AtomicLong(0);
                    session.executeReadTable("/local/issues", ReadTableSettings.newBuilder().build())
                            .start(
                                    readTablePart -> {
                                        var resultSetReader = readTablePart.getResultSetReader();
                                        while (resultSetReader.next()) {
                                            ValueReader column = resultSetReader.getColumn(linkCountColumnName);
                                            long links = column.getInt64();
                                            if (links > 0) count.incrementAndGet();
                                        }
                                    }
                            ).join().expectSuccess();
                    return CompletableFuture.completedFuture(Result.success(count));
                }
        ).join().getValue();
    }

    /**
     * Чтение данных из таблицы по ключу.
     * Использует readRows для получения записей по конкретному id.
     */
    public List<Issue> readRows(String tableName, long id) {
        var keyStruct = StructType.of(idColumnName, PrimitiveType.Int64);

        return retryTableCtx.supplyResult(session -> {
                    var listResult = new ArrayList<Issue>();

                    var resultSetReader = session.readRows(tableName,
                            ReadRowsSettings.newBuilder()
                                    .addKey(keyStruct.newValue(idColumnName, PrimitiveValue.newInt64(id)))
                                    .addColumns(idColumnName, titleColumnName, createdAtColumnName, authorColumnName)
                                    .build()
                    ).join().getValue().getResultSetReader();

                    fetchIssues(listResult, resultSetReader);

                    return CompletableFuture.completedFuture(Result.success(listResult));
                }
        ).join().getValue();
    }

    /**
     * Вспомогательный метод для преобразования результатов запроса в объекты Issue.
     * Обрабатывает различные варианты структуры данных (с link_count и status или без них).
     */
    private void fetchIssues(ArrayList<Issue> listResult, ResultSetReader resultSetReader) {
        while (resultSetReader.next()) {
            var id = resultSetReader.getColumn(resultSetReader.getColumnIndex(idColumnName)).getInt64();

            int linkCountColumnIndex = resultSetReader.getColumnIndex(linkCountColumnName);
            long linksCount = linkCountColumnIndex > -1
                    ? resultSetReader.getColumn(linkCountColumnIndex).getInt64()
                    : 0;

            int statusColumnIndex = resultSetReader.getColumnIndex(statusColumnName);
            String status = statusColumnIndex > -1
                    ? resultSetReader.getColumn(statusColumnIndex).getText()
                    : "";

            int titleColumnIndex = resultSetReader.getColumnIndex(titleColumnName);
            String title = titleColumnIndex > -1
                    ? resultSetReader.getColumn(titleColumnIndex).getText()
                    : null;
            if (title == null)
                throw new NullPointerException("NULL value provided for mandatory field 'title'. The field is mapped to a database column with a NOT NULL constraint.");

            int createdAtColumnIndex = resultSetReader.getColumnIndex(createdAtColumnName);
            Instant createdAt = createdAtColumnIndex > -1
                    ? resultSetReader.getColumn(createdAtColumnIndex).getTimestamp()
                    : null;
            if (createdAt == null)
                throw new NullPointerException("NULL value provided for mandatory field 'created_at'. The field is mapped to a database column with a NOT NULL constraint.");

            int authorColumnIndex = resultSetReader.getColumnIndex(authorColumnName);
            String author = authorColumnIndex > -1
                    ? resultSetReader.getColumn(authorColumnIndex).getText()
                    : "";

            listResult.add(
                    new Issue(
                            id,
                            title,
                            createdAt,
                            author,
                            linksCount,
                            status
                    )
            );
        }
    }
}
