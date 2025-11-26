package tech.ydb.app;

import tech.ydb.query.QueryTransaction;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;

/**
 * @author Kirill Kurdyukov
 */
public class TransactionHelper {

    private final QueryTransaction transaction;

    public TransactionHelper(QueryTransaction transaction) {
        this.transaction = transaction;
    }

    public void executeQuery(String yql, Params params) {
        QueryReader.readFrom(transaction.createQueryWithCommit(yql, params)).join().getValue();
    }

    public void executeQuery(String yql) {
        QueryReader.readFrom(transaction.createQueryWithCommit(yql)).join().getValue();
    }

    public QueryReader executeQueryWithCommit(String yql, Params params) {
        return QueryReader.readFrom(transaction.createQueryWithCommit(yql, params)).join().getValue();
    }

    public QueryTransaction getTransaction() {
        return transaction;
    }
}
