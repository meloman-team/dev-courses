package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.time.Duration;

/**
 * Тема 1/1: Работа с таблицей → Урок 3/4
 * Практика. Создание строковых таблиц
 * Таблица с TTL 1 час
 * 1️⃣ Создайте таблицу с TTL 1 час.
 * 2️⃣ Добавьте в таблицу 10 000 строк. В первой строке значение времени в поле TTL — «сейчас», во второй — на 1 секунду больше и так далее.
 * 3️⃣ Добавьте одну строку со значением NULL в TTL-ко.
 * 4️⃣ Через 2–3 часа проверьте, какие строки удалились.
 */
public class ApplicationTTL {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationTTL.class);
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build()
        ) {
            try (QueryClient queryClient = QueryClient.newClient(grpcTransport).build()) {
                var retryCtx = SessionRetryContext.create(queryClient).build();

                var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
                var issueYdbRepository = new OperationYdbRepository(retryCtx);

                schemaYdbRepository.dropSchema();
                schemaYdbRepository.createSchema();
                schemaYdbRepository.configureAutoPartitioning();

                var firstIssue = issueYdbRepository.addOperation("Operation 1");

                LOGGER.debug("Create operation with id: \"{}\"", firstIssue.operationId());

                var twoIssue = issueYdbRepository.addOperationTtlNull("Eternal operation");

                LOGGER.debug("Create operation no TTL with id: \"{}\"", twoIssue.operationId());

            }
        }
    }
}
