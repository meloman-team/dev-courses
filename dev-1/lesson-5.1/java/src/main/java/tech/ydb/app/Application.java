package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Пример работы с индексами в YDB: создание и использование вторичных индексов
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

                issueYdbRepository.addIssue("Ticket 1", "Author 1");
                issueYdbRepository.addIssue("Ticket 2", "Author 2");
                issueYdbRepository.addIssue("Ticket 3", "Author 3");
                addIssues(issueYdbRepository);

                var allIssues = issueYdbRepository.findAll();

                LOGGER.info("Print all tickets: ");
                for (var issue : allIssues) {
                    printIssue(issue);
                }

                // Демонстрация поиска по вторичному индексу authorIndex
                LOGGER.info("Find by index `authorIndex`: ");
                printIssue(issueYdbRepository.findByAuthor("Author 2"));

                // Демонстрация поиска по вторичному индексу authorAndCreatedAtIndex
                LOGGER.info("Find by index `authorAndCreatedAtIndex`: ");
                printIssue("First 10", issueYdbRepository.findByAuthorFirst10("Author 2"));

                // Демонстрация поиска по вторичному индексу authorAndCreatedAtIndex
                LOGGER.info("Find by index `authorAndCreatedAtIndex`: ");
                printIssue("Last 10", issueYdbRepository.findByAuthorLast10("Author 2"));
            }
        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }

    private static void printIssue(String prefix, List<Issue> issues) {
        String listIssue = issues.stream().map(Record::toString).collect(Collectors.joining("\n"));
        LOGGER.info(prefix + ": {}", listIssue);
    }

    private static void addIssues(IssueYdbRepository repository) {
        for (int i = 1; i < 30; i++) {
            repository.addIssue("Ticket 2-" + i, "Author 2");
        }
    }
}
