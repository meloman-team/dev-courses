package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;

import java.time.Duration;

/**
 * Пример работы с топиками в YDB
 *
 * @author Kirill Kurdyukov
 */
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()
        ) {
            var retryCtx = SessionRetryContext.create(queryClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();

            issueYdbRepository.addIssue("Ticket 1", "Author 1");
            issueYdbRepository.addIssue("Ticket 2", "Author 2");
            issueYdbRepository.addIssue("Ticket 3", "Author 3");

            var allIssues = issueYdbRepository.findAll();

            LOGGER.info("Print all tickets: ");
            for (var issue : allIssues) {
                printIssue(issue);
            }

            // Создаем сервис для обновления статусов тикетов через топики
            var updateService = new StatusUpdateService(topicClient, issueYdbRepository);

            LOGGER.info("Update status all tickets: NULL -> OPEN ");
            for (var issue : allIssues) {
//                updateService.update(issue.id(), "OPEN");
                updateService.updateTx(issue.id(), "OPEN");
            }

            // Запускаем воркер для чтения сообщений из топика в отдельном потоке
            // он будет получать события об обновлении тикетов и эмулировать отправку 
            // уведомлений
            var readerWorker = new ReaderWorker(topicClient, issueYdbRepository);
            readerWorker.run();

            LOGGER.info("Update status all tickets: OPEN -> IN_PROGRESS ");
            for (var issue : allIssues) {
//                updateService.update(issue.id(), "IN_PROGRESS");
                updateService.updateTx(issue.id(), "IN_PROGRESS");
            }

            // Корректно завершаем работу сервисов
            updateService.shutdown();
            readerWorker.shutdown();

            LOGGER.info("Print all issues: ");
            for (var ticket : issueYdbRepository.findAll()) {
                printIssue(ticket);
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }
}
