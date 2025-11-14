package tech.ydb.app;

import tech.ydb.query.tools.SessionRetryContext;

/**
 * @author Kirill Kurdyukov
 */
public class SchemaYdbRepository {

    private final QueryServiceHelper queryServiceHelper;

    public SchemaYdbRepository(SessionRetryContext retryCtx) {
        this.queryServiceHelper = new QueryServiceHelper(retryCtx);
    }

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

        queryServiceHelper.executeQuery("ALTER TABLE issues ADD INDEX authorIndex GLOBAL ON (author);");

        queryServiceHelper.executeQuery("""
                ALTER TABLE issues ADD COLUMN link_count Int64;
                CREATE TABLE IF NOT EXISTS links (
                    source Int64 NOT NULL,
                    destination Int64 NOT NULL,
                    PRIMARY KEY(source, destination)
                );
                """
        );

        queryServiceHelper.executeQuery("""
                CREATE TOPIC IF NOT EXISTS task_status(
                    CONSUMER email
                ) WITH(
                    auto_partitioning_strategy='scale_up',
                    min_active_partitions=2,
                    max_active_partitions=10,
                    retention_period = Interval('P3D')
                );
                                                    
                ALTER TABLE issues ADD COLUMN status Text;
                """
        );
    }

    public void createEventsTopic() {
        queryServiceHelper.executeQuery("""
                CREATE TOPIC IF NOT EXISTS events(
                    CONSUMER ConsumerEvents
                ) WITH(
                    auto_partitioning_strategy='scale_up',
                    min_active_partitions=2,
                    max_active_partitions=10,
                    retention_period = Interval('P3D')
                );
                """
        );
    }

    public void dropSchema() {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS issues;
                DROP TABLE IF EXISTS links;
                DROP TOPIC IF EXISTS task_status;
                """
        );
    }

    public void dropEventsTopic() {
        queryServiceHelper.executeQuery("""
                DROP TOPIC IF EXISTS events;
                """
        );
    }
}
