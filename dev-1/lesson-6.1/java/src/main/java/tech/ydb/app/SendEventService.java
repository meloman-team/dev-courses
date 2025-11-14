package tech.ydb.app;

import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Сервис для отправки сообщений через топики YDB
 */
public class SendEventService {
    private final SyncWriter writer;

    public SendEventService(TopicClient topicClient) {
        // Создаем синхронный writer для отправки сообщений в топик task_status
        this.writer = topicClient.createSyncWriter(
                WriterSettings.newBuilder()
                        .setProducerId("producer-events")
                        .setTopicPath("events")
                        .build()
        );
        this.writer.init();
    }

    public void sendEvent(String text) {
        // Отправляем сообщение об обновлении статуса в топик
        writer.send(Message.newBuilder()
                .setData(text.getBytes(StandardCharsets.UTF_8))
                .build()
        );
        writer.flush(); // ждем отправки
    }

    public void shutdown() {
        try {
            // Корректно завершаем работу writer'а
            writer.shutdown(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
