package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Тема 1/1: Работа с таблицей → Урок 3/4
 * Практика. Создание строковых таблиц
 * Таблица с включённым шардированием
 * 1️⃣ Создайте таблицу c включённым шардированием по размеру и максимальным размером шарда 1 МБ.
 * 2️⃣ Проверьте во встроенном UI, что таблица создалась с одним шардом. Размер шарда — 1 МБ. Для этого откройте в браузере страницу <a href="http://localhost:8765">...</a> и нажмите внизу на ссылку /local.
 * 3️⃣ Напишите и запустите программу для вставку в таблицу 2-3 Мб данных
 * Можно использовать тип Bytes или Text и вставлять большой объём данных в каждую строку, например по 100 Кб. Тогда нужный объём можно будет набрать на небольшом количестве строк, которые вставятся быстро.
 * 4️⃣ Проверьте, что количество партиций в таблице увеличилось
 */
public class ApplicationShards {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationShards.class);
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

                int targetTotalMB = 3;
                int stringSizeKB = 100;
                int totalStrings = (targetTotalMB * 1024 + stringSizeKB - 1) / stringSizeKB; // Округление вверх

                System.out.println("Нужно строк: " + totalStrings);

                for (int i = 0; i < totalStrings; i++) {
                    var firstIssue = issueYdbRepository.addOperation(stringGenerator());
                    LOGGER.debug("{}: Create operation with id: \"{}\"", i, firstIssue.operationId());
                }

            }
        }
    }

    /**
     * @return Строка из символа "A" размером 100кб
     */
    private static String stringGenerator() {
        int targetSizeInBytes = 100 * 1024; // 100 КБ
        StringBuilder sb = new StringBuilder();

        while (true) {
            String testString = sb.toString();
            byte[] bytes = testString.getBytes(StandardCharsets.UTF_8);
            if (bytes.length >= targetSizeInBytes) {
                break;
            }
            sb.append('A');
        }

        String result = sb.toString();
        LOGGER.debug("Итоговый размер: {} байт", result.getBytes(StandardCharsets.UTF_8).length);
        return result;
    }

}
