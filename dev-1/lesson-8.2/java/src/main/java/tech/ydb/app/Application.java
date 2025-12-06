package tech.ydb.app;

import com.opencsv.CSVReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * @author Kirill Kurdyukov
 */
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String PATH = "dev-1/lesson-8.2/java/title_author.csv";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";

    public static void main(String[] args) {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             TableClient tableClient = TableClient.newClient(grpcTransport).build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build()
        ) {
            var retryCtx = SessionRetryContext.create(queryClient).build();
            var retryTableCtx = tech.ydb.table.SessionRetryContext.create(tableClient).build();

            var schemaYdbRepository = new SchemaYdbRepository(retryCtx);
            var issueYdbRepository = new IssueYdbRepository(retryCtx);
            var nativeApiYdbRepository = new KeyValueApiYdbRepository(retryTableCtx);

            schemaYdbRepository.dropSchema();
            schemaYdbRepository.createSchema();

            List<Issue> issues = createNewIssueFromFile();

            // Массовое добавление данных через Key-Value API
            nativeApiYdbRepository.bulkUpsert("/local/issues", issues);

            Issue lastIssue = null;
            Issue firstIssue = null;
            LOGGER.info("Print all issues: ");
            for (var issue : issueYdbRepository.findAll()) {
                printIssue(issue);

                lastIssue = issue;
                if (firstIssue == null) firstIssue = issue;
            }

            // Чтение всех данных через Key-Value API
            LOGGER.info("ReadTable: ");
            for (var issue : nativeApiYdbRepository.readTable("/local/issues")) {
                printIssue(issue);
            }

            // Чтение данных по ключу через Key-Value API
            LOGGER.info("ReadRows: ");
            assert lastIssue != null;
            for (var issue : nativeApiYdbRepository.readRows("/local/issues", lastIssue.id())) {
                printIssue(issue);
            }

            AtomicLong countLink = nativeApiYdbRepository.readTableCountLink();
            LOGGER.info("Count link: {}", countLink.get()); // ожидаем 2

            // Не стал писать извлечение из файла, т.к. id в таблицах issue и links будут не консистентны
            // Как вариант вручную проставлять id в обоих файлах и не использовать генерацию id но мне лень :)
            // Все таки курс про YDB а не про работу с файлами...
            List<Link> links = new ArrayList<>();
            links.add(new Link(lastIssue.id(), firstIssue.id()));
            nativeApiYdbRepository.bulkUpsertLinks(links);

        }
    }

    private static void printIssue(Issue issue) {
        LOGGER.info("Issue: {}", issue);
    }

    private static List<Issue> createNewIssueFromFile() {
        var result = new ArrayList<Issue>();
        try (CSVReader reader = new CSVReader(Files.newBufferedReader(
                Path.of(System.getProperty("user.dir"), PATH)))
        ) {
            reader.readNext(); // Пропускаем названия столбцов
            reader.forEach(row -> { // обрабатываем данные столбцов
                var title = row[0];
                var author = row[1];
                var linkCounts = Long.parseLong(row[2]);
                result.add(new Issue(
                        ThreadLocalRandom.current().nextLong(),
                        title,
                        Instant.now(),
                        author,
                        linkCounts,
                        ""
                ));
            });
        } catch (CsvValidationException | IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return result;
    }
}
