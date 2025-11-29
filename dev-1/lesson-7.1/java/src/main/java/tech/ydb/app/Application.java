package tech.ydb.app;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;

/**
 * Пример работы с changefeed в YDB
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

            // Создаём тикеты, информация об этом попадёт в changefeed-топик
            var first = issueYdbRepository.addIssue("Ticket 1", "Author 1");
            var second = issueYdbRepository.addIssue("Ticket 2", "Author 2");

            // Обновляем статус первого тикета - это изменение тоже попадёт в changefeed-топик
            issueYdbRepository.updateStatus(first.id(), "future");

            // Удаляем второй тикет - это изменение также будет отслежено через changefeed
            issueYdbRepository.delete(second.id());

            // Тут удаляется несуществующий тикет.
            // Несмотря на то что запись в таблице отсутствует - операция всё равно будет записана
            // в changefeed-топик ещё раз.
            issueYdbRepository.delete(second.id());

            issueYdbRepository.linkTicketsNoInteractive(first.id(), second.id());
            issueYdbRepository.delTicketsNoInteractive(first.id(), second.id());

            // Запускаем воркер для чтения изменений из changefeed
            var readerWorker = new ReaderChangefeedWorker(topicClient);
            readerWorker.readChangefeed();

            LOGGER.info("Print all tickets: ");
            for (var issue : issueYdbRepository.findAll()) {
                printIssue(issue);
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }
}
