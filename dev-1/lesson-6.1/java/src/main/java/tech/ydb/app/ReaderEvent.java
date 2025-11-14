package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Воркер для чтения сообщений из топика YDB
 */
public class ReaderEvent {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final SyncReader reader;
    private final String readerName;

    public ReaderEvent(TopicClient topicClient, String readerName) {
        // Создаем синхронный reader для чтения сообщений из топика task_status
        this.readerName = readerName;
        this.reader = topicClient.createSyncReader(
                ReaderSettings.newBuilder()
                        .setConsumerName("ConsumerEvents")
                        .setReaderName(readerName)
                        .setTopics(List.of(TopicReadSettings.newBuilder().setPath("events").build()))
                        .build()
        );

        reader.init();
    }

    public void read() {
        CompletableFuture.runAsync(
                () -> {
                    try {
                        // Читаем сообщение с таймаутом в 5 секунд по умолчанию.
                        // Если за 5 секунд сообщение не будет получено, метод повторно будет запрашивать сообщение до успешного получения.
                        var message = reader.receive();

                        // Выводим полученное сообщение
                        LOGGER.info("Read message {}: {}", readerName, new String(message.getData()));

                    } catch (Exception e) {
                        LOGGER.error("Error: ", e);
                    }
                }).join();
    }

    public void readAndCommit() {
        CompletableFuture.runAsync(
                () -> {
                    try {
                        // Читаем сообщение с таймаутом в 5 секунд по умолчанию.
                        // Если за 5 секунд сообщение не будет получено, метод повторно будет запрашивать сообщение до успешного получения.
                        var message = reader.receive();
                        message.commit().join();

                        // Выводим полученное сообщение
                        LOGGER.info("Read message {}: {}", readerName, new String(message.getData()));

                    } catch (Exception e) {
                        LOGGER.error("Error: ", e);
                    }
                }).join();
    }

    public void shutdown() {
        reader.shutdown();
    }
}
