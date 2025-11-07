package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.time.Duration;

/**
 * Пример работы с транзакциями в YDB, урок - 4.1 Распределенные транзакции
 *
 * @author Kirill Kurdyukov
 */
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    // Строка подключения к локальной базе данных YDB
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
                var issueYdbRepository = new IssueYdbRepository(retryCtx);

                schemaYdbRepository.dropSchema();
                schemaYdbRepository.createSchema();

                // Создаем тикеты с авторами
                issueYdbRepository.addIssue("Ticket 1", "Author 1");
                issueYdbRepository.addIssue("Ticket 2", "Author 2");
                issueYdbRepository.addIssue("Ticket 3", "Author 3");

                var allIssues = issueYdbRepository.findAll();

                LOGGER.info("Print all tickets: ");
                for (var issue : allIssues) {
                    printIssue(issue);
                }

                var first = allIssues.get(0);
                var second = allIssues.get(1);

                // Демонстрация неинтерактивной транзакции - все запросы выполняются за один запрос к YDB
                LOGGER.info("Linked tickets by non-interactive transactions id1 = {}, id2 = {}", first.id(), second.id());
                var result1 = issueYdbRepository.linkTicketsNoInteractive(first.id(), second.id());
                LOGGER.info("Result operation:");
                for (var v : result1) {
                    LOGGER.info("Id = {}, linkCounts = {}", v.id(), v.linkCount());
                }

                var third = allIssues.get(2);
                // Демонстрация интерактивной транзакции - между запросами к YDB есть логика на стороне приложения
                LOGGER.info("Linked tickets by interactive transactions id2 = {}, id3 = {}", second.id(), third.id());
                var result2 = issueYdbRepository.linkTicketsInteractive(second.id(), third.id());
                LOGGER.info("Result operation:");
                for (var v : result2) {
                    LOGGER.info("Id = {}, linkCounts = {}", v.id(), v.linkCount());
                }

                LOGGER.info("Print all tickets: ");
                for (var ticket : issueYdbRepository.findAll()) {
                    printIssue(ticket);
                }

                issueYdbRepository.delTicketsNoInteractive(first.id(), second.id());
                issueYdbRepository.delTicketsInteractive(third.id(), second.id());
//                issueYdbRepository.delTicketsNoInteractive(third.id(), first.id()); // Бросает исключение при проверке устловия Ensure
//                issueYdbRepository.delTicketsInteractive(third.id(), first.id()); // Бросает исключение при проверке устловия Ensure
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }
}
