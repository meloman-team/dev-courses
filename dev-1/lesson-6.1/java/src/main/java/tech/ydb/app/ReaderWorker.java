package tech.ydb.app;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 * Воркер для чтения сообщений из топика YDB
 *
 * @author Kirill Kurdyukov
 */
public class ReaderWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final IssueYdbRepository issueYdbRepository;
    private final SyncReader reader;
    private volatile CompletableFuture<Void> readerJob;

    public ReaderWorker(TopicClient topicClient, IssueYdbRepository issueYdbRepository) {
        this.issueYdbRepository = issueYdbRepository;
        // Создаем синхронный reader для чтения сообщений из топика task_status
        this.reader = topicClient.createSyncReader(
                ReaderSettings.newBuilder()
                        .setConsumerName("email")
                        .setTopics(List.of(TopicReadSettings.newBuilder().setPath("task_status").build()))
                        .build()
        );

        reader.init();
    }

    public void run() {
        // Запускаем фоновое чтение сообщений из топика
        readerJob = CompletableFuture.runAsync(
                () -> {
                    LOGGER.info("Started read worker!");

                    while (true) {
                        try {
                            // Читаем сообщение с таймаутом в 1 секунду.
                            // Если за 1 секунду сообщение не будет получено, метод вернет null
                            // и цикл продолжит работу, ожидая следующее сообщение.
                            // Таймаут, чтобы остановить чтение топика по сигналу в stoppedProcess.
//                            var message = reader.receive(1, TimeUnit.SECONDS);
                            final Message message = issueYdbRepository.incChangeStatusTx((tx) -> {
                                        try {
                                            // Читаем сообщение в рамках транзакции
                                            return reader.receive(ReceiveSettings.newBuilder()
                                                    .setTransaction(tx.getTransaction())
                                                    .setTimeout(1, TimeUnit.SECONDS)
                                                    .build()
                                            );
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                            );

                            if (message == null) {
                                continue;
                            }

                            // Выводим полученное сообщение
                            LOGGER.info("Received message: {}", new String(message.getData()));

                            if (message.getSeqNo() == 6) {
                                break; // в примере мы вычитываем 6 обновлений
                            }
                        } catch (Exception e) {
                            LOGGER.warn(e.getMessage());
                        }
                    }

                    LOGGER.info("Stopped read worker!");
                }
        );
    }

    public void shutdown() {
        readerJob.join();
        reader.shutdown();
    }
}
