package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.topic.TopicClient;

import java.time.Duration;

/**
 *  Практика. Отправка сообщений в топик и их чтение из него
 *  Создайте новый топик — events.
 *  Добавьте к функциям работы с задачами возможность отправки сообщений в топик в виде лога.
 *  Напишите команду для многократного просмотра этого лога и скрытия прочитанных сообщений: вариант чтения с коммитом и без коммита сообщений.
 */
public class ApplicationTestRead {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationTestRead.class);
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

            schemaYdbRepository.dropEventsTopic();
            schemaYdbRepository.createEventsTopic();

            SendEventService sender = new SendEventService(topicClient);
            ReaderEvent reader = new ReaderEvent(topicClient, "reader1");


            // пишем 2 сообщения в топик
            sender.sendEvent("1 message");
            LOGGER.info("Send message 1");
            sender.sendEvent("2 message");
            LOGGER.info("Send message 2");

            // читаем 2 сообщения без комита
            reader.read();
            reader.read();
            reader.shutdown();

            sender.sendEvent("3 message");
            LOGGER.info("Send message 3");
            sender.sendEvent("4 message");
            LOGGER.info("Send message 4");

            // проверяем что второму ридеру доступны те же сообщения
            ReaderEvent reader2 = new ReaderEvent(topicClient, "reader2");
            reader2.readAndCommit();
            reader2.readAndCommit();
            reader2.shutdown();

            // проверяем что reader3 получит только незакомиченные сообщения 3 и 4
            ReaderEvent reader3 = new ReaderEvent(topicClient, "reader3");
            reader3.readAndCommit();
            reader3.readAndCommit();
            reader3.shutdown();

            sender.shutdown();
        }
    }

}
