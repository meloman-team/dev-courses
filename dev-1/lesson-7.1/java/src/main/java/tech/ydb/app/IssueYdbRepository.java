package tech.ydb.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.values.PrimitiveValue;

/**
 * @author Kirill Kurdyukov
 */
public class IssueYdbRepository {
    private final QueryServiceHelper queryServiceHelper;

    public IssueYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

    public void updateStatus(long id, String status) {
        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DECLARE $new_status AS Text;
                                                            
                        UPDATE issues SET status = $new_status WHERE id = $id;
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$id", PrimitiveValue.newInt64(id),
                        "$new_status", PrimitiveValue.newText(status))
        );
    }

    public List<IssueLinkCount> linkTicketsNoInteractive(long idT1, long idT2) {
        var valueReader = queryServiceHelper.executeQuery("""
                        DECLARE $t1 AS Int64;
                        DECLARE $t2 AS Int64;
                                                            
                        UPDATE issues
                        SET link_count = COALESCE(link_count, 0) + 1
                        WHERE id IN ($t1, $t2);
                                                            
                        INSERT INTO links (source, destination)
                        VALUES ($t1, $t2), ($t2, $t1);

                        SELECT id, link_count FROM issues
                        WHERE id IN ($t1, $t2)
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
        );

        return getLinkTicketPairs(valueReader);
    }

    public void delTicketsNoInteractive(long idT1, long idT2) {
        queryServiceHelper.executeQuery(
                """
                        DECLARE $t1 AS Int64;
                        DECLARE $t2 AS Int64;
                                               
                        -- Удаляем записи о связях между тикетами
                        DELETE FROM links
                        WHERE (source = $t1 and destination = $t2) 
                           OR (source = $t2 and destination = $t1);
                                                 
                        -- Обновляем счетчики связей
                        UPDATE issues
                        SET link_count = COALESCE(link_count, 0) - 1
                        WHERE id IN ($t1, $t2);
                                       
                        -- Валидация счетчиков после удаления       
                        SELECT Ensure(
                            link_count,
                            link_count >= 0,
                            "value out or range"
                        ) AS value FROM issues;
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
        );
    }

    public List<IssueLinkCount> linkTicketsInteractive(long idT1, long idT2) {
        return queryServiceHelper.executeInTx(TxMode.SERIALIZABLE_RW,
                tx -> {
                    tx.executeQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                 
                                    UPDATE issues
                                    SET link_count = COALESCE(link_count, 0) + 1
                                    WHERE id IN ($t1, $t2);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    );

                    tx.executeQuery("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                        
                                    INSERT INTO links (source, destination)
                                    VALUES ($t1, $t2), ($t2, $t1);
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    );

                    var valueReader = tx.executeQueryWithCommit("""
                                    DECLARE $t1 AS Int64;
                                    DECLARE $t2 AS Int64;
                                                                        
                                    SELECT id, link_count FROM issues
                                    WHERE id IN ($t1, $t2)
                                    """,
                            Params.of("$t1", PrimitiveValue.newInt64(idT1), "$t2", PrimitiveValue.newInt64(idT2))
                    );

                    return getLinkTicketPairs(valueReader);
                }
        );
    }

    public Issue addIssue(String title, String author) {
        var id = ThreadLocalRandom.current().nextLong();
        var now = Instant.now();

        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DECLARE $title AS Text;
                        DECLARE $created_at AS Timestamp;
                        DECLARE $author AS Text;
                        UPSERT INTO issues (id, title, created_at, author)
                        VALUES ($id, $title, $created_at, $author);
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of(
                        "$id", PrimitiveValue.newInt64(id),
                        "$title", PrimitiveValue.newText(title),
                        "$created_at", PrimitiveValue.newTimestamp(now),
                        "$author", PrimitiveValue.newText(author)
                )
        );

        return new Issue(id, title, now, author, 0, null);
    }

    public void delete(long id) {
        queryServiceHelper.executeQuery("""
                        DECLARE $id AS Int64;
                        DELETE FROM issues WHERE id=$id;
                        """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$id", PrimitiveValue.newInt64(id))
        );
    }

    public List<Issue> findAll() {
        var titles = new ArrayList<Issue>();
        var resultSet = queryServiceHelper.executeQuery(
                "SELECT id, title, created_at, author, COALESCE(link_count, 0), status FROM issues;",
                TxMode.SNAPSHOT_RO,
                Params.empty()
        );

        var resultSetReader = resultSet.getResultSet(0);

        while (resultSetReader.next()) {
            titles.add(new Issue(
                    resultSetReader.getColumn(0).getInt64(),
                    resultSetReader.getColumn(1).getText(),
                    resultSetReader.getColumn(2).getTimestamp(),
                    resultSetReader.getColumn(3).getText(),
                    resultSetReader.getColumn(4).getInt64(),
                    resultSetReader.getColumn(5).getText()
            ));
        }

        return titles;
    }

    public Issue findByAuthor(String author) {
        var resultSet = queryServiceHelper.executeQuery("""
                        DECLARE $author AS Text;
                        SELECT id, title, created_at, author, COALESCE(link_count, 0), status FROM issues
                        WHERE author = $author;
                        """,
                TxMode.SNAPSHOT_RO,
                Params.of("$author", PrimitiveValue.newText(author))
        );

        var resultSetReader = resultSet.getResultSet(0);
        resultSetReader.next();

        return new Issue(
                resultSetReader.getColumn(0).getInt64(),
                resultSetReader.getColumn(1).getText(),
                resultSetReader.getColumn(2).getTimestamp(),
                resultSetReader.getColumn(3).getText(),
                resultSetReader.getColumn(4).getInt64(),
                resultSetReader.getColumn(5).getText()
        );
    }

    private static List<IssueLinkCount> getLinkTicketPairs(QueryReader valueReader) {
        var linkTicketPairs = new ArrayList<IssueLinkCount>();
        var resultSet = valueReader.getResultSet(0);

        while (resultSet.next()) {
            linkTicketPairs.add(new IssueLinkCount(resultSet.getColumn(0).getInt64(), resultSet.getColumn(1).getInt64()));
        }
        return linkTicketPairs;
    }
}
