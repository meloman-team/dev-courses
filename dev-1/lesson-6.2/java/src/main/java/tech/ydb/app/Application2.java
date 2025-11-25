package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Пример для проверки логики с таблицей file_progress и однократной обработкой сообщений
 */
public class Application2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application2.class);
    private static final String PATH = "dev-1/lesson-6.2/java/file.txt";
//    private static final String PATH = "dev-1/lesson-6.2/java/video.mp4";
    private static final String CONNECTION_STRING = "grpc://localhost:2136/local";
    private static final String SEPARATOR = "<sep>"; // Двоеточие как в примере ненадежный вариант разделителя для тестирования реальных файлов, например видео, символ двоеточия может попадаться в файле и деление будет хаотичным.
//    private static final int CHUNK_SIZE = 100000; // Размер блока файла в байтах
    private static final int CHUNK_SIZE = 10; // Размер блока файла в байтах

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        try (GrpcTransport grpcTransport = GrpcTransport
                .forConnectionString(CONNECTION_STRING)
                .withConnectTimeout(Duration.ofSeconds(10))
                .build();
             QueryClient queryClient = QueryClient.newClient(grpcTransport).build();
             TopicClient topicClient = TopicClient.newClient(grpcTransport).build()
        ) {

            var retryCtx = SessionRetryContext.create(queryClient).build();
            QueryServiceHelper queryServiceHelper = new QueryServiceHelper(retryCtx);

//            dropSchema(queryServiceHelper); // Комментируем, для выполнения тестов на прерывание процесса, перед каждым тестом выполняем очистку базы вручную.
            createSchema(queryServiceHelper);

            // Создаем writer для отправки строк файла в топик
            SyncWriter writer = topicClient.createSyncWriter(
                    WriterSettings.newBuilder()
                            .setProducerId("producer-file")
                            .setTopicPath("file_topic")
                            .build()
            );

            // Создаем reader для чтения строк из топика
            var reader = topicClient.createSyncReader(
                    ReaderSettings.newBuilder()
                            .setConsumerName("file_consumer")
                            .setTopics(List.of(TopicReadSettings.newBuilder().setPath("file_topic").build()))
                            .build()
            );

            writer.init();
            reader.init();

            long sendMessages = sendMessages(queryServiceHelper, writer);

            // Запускаем фоновое чтение строк из топика
            var readerJob = CompletableFuture.runAsync(() -> runReadJob(sendMessages, reader, retryCtx));

            readerJob.join();
            printTableFile(queryServiceHelper);

            writer.shutdown(10, TimeUnit.SECONDS);
            reader.shutdown();
        }
    }

    /**
     * @return Количество отправленных сообщений
     */
    private static long sendMessages(QueryServiceHelper queryServiceHelper, SyncWriter writer) {
        String currentDirectory = System.getProperty("user.dir");
        Path pathFile = Path.of(currentDirectory, PATH);
        // Получаем номер последнего обработанного блока из таблицы прогресса
        QueryReader queryReader = queryServiceHelper.executeQuery("""
                            DECLARE $name AS Text;
                            SELECT line_num FROM write_file_progress
                            WHERE name = $name;
                            """,
                TxMode.SERIALIZABLE_RW,
                Params.of("$name", PrimitiveValue.newText(pathFile.toString()))
        );

        ResultSetReader resultSet = queryReader.getResultSet(0);

        long chunkNumberLastLong = 1;
        if (resultSet.next()) {
            chunkNumberLastLong = resultSet.getColumn(0).getInt64();
        }

        AtomicLong chunkNumberLast = new AtomicLong(chunkNumberLastLong);
        AtomicLong chunkNumber = new AtomicLong(1);
        byte[] chunkFile = new byte[CHUNK_SIZE];

        try (InputStream inputStream = Files.newInputStream(pathFile)) {
            int bytesRead;
            while ((bytesRead = inputStream.read(chunkFile)) != -1) {
                String text = new String(chunkFile, 0, bytesRead);

                if (chunkNumber.getAndIncrement() < chunkNumberLast.get()) {
                    break;
                }

                // Порядковый номер блока файла используется в качестве
                // порядкового номера сообщения внутри Producer'а
                // таким образом при перезапуске процесса сообщения будут повторяться
                // так, что сервер сможет правильно пропустить дубли.
                writer.send(Message.newBuilder()
                        .setSeqNo(chunkNumberLast.getAndIncrement())
                        .setData((PATH + SEPARATOR + text).getBytes(StandardCharsets.UTF_8))
                        .build()
                );
                writer.flush();

                // Сохраняем прогресс обработки файла в таблицу, чтобы
                // не повторять работу при перезапусках
                queryServiceHelper.executeQuery("""
                                        DECLARE $name AS Text;
                                        DECLARE $line_num AS Int64;
                                                                                
                                        UPSERT INTO write_file_progress(name, line_num) VALUES ($name, $line_num);
                                        """,
                        TxMode.SERIALIZABLE_RW,
                        Params.of("$name", PrimitiveValue.newText(pathFile.toString()),
                                "$line_num", PrimitiveValue.newInt64(chunkNumberLast.get()))
                );
            }
        } catch (IOException e) {
            LOGGER.error("Error: ", e);
        }
        return chunkNumberLast.get();
    }

    private static void runReadJob(long linesCount, SyncReader reader, SessionRetryContext retryCtx) {
        LOGGER.info("Started read worker! {}", linesCount);

        for (int i = 0; i < linesCount; i++) {
            try {
                var message = reader.receive(2, TimeUnit.SECONDS);

                // Далее идёт пример обработки сообщения из топика ровно 1 раз без использования
                // транзакций для объединения операции с таблицами и топиками - т.е. то, как это
                // делается в других системах, когда нет готовых интеграций.

                // В транзакции будет проверяться было ли уже обработано сообщение и если нет, то
                // обрабатывать и сохранять прогресс. Эти операции должны быть атомарными, чтобы
                // избежать ситуации, когда одно и тоже сообщение будет обработано несколько раз.
                retryCtx.supplyStatus(session -> {
                    var curTx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
                    var tx = new TransactionHelper(curTx);
                    assert message != null;
                    var partitionId = message.getPartitionSession().getPartitionId();

                    // Проверяем, не обрабатывали ли мы уже это сообщение
                    var queryReader = tx.executeQuery("""
                                        DECLARE $partition_id AS Int64;
                                        SELECT last_offset FROM file_progress
                                        WHERE partition_id = $partition_id;
                                    """,
                            Params.of("$partition_id", PrimitiveValue.newInt64(partitionId))
                    );

                    var resultSet = queryReader.getResultSet(0);
                    var lastOffset = resultSet.next() ? resultSet.getColumn(0).getInt64() : 0;

                    // Если сообщение уже было обработано, пропускаем его
                    if (lastOffset > message.getOffset()) {
                        message.commit().join();
                        curTx.rollback().join();

                        return CompletableFuture.completedFuture(Status.SUCCESS);
                    }

                    var messageData = new String(message.getData(), StandardCharsets.UTF_8).split(SEPARATOR);
                    var name = messageData[0];
                    var length = messageData[1].length();
                    var lineNumber = message.getSeqNo();

                    // Сохраняем информацию о строке в таблицу
                    tx.executeQuery("""
                                        DECLARE $name AS Text;
                                        DECLARE $line AS Int64;
                                        DECLARE $length AS Int64;
                                        UPSERT INTO file(name, line, length) VALUES ($name, $line, $length);
                                    """,
                            Params.of(
                                    "$name", PrimitiveValue.newText(name),
                                    "$line", PrimitiveValue.newInt64(lineNumber),
                                    "$length", PrimitiveValue.newInt64(length)
                            )
                    );

                    // Обновляем прогресс обработки партиции. Если произойдёт сбой после коммита транзакции,
                    // но до коммита сообщения в топик, то на основе этого прогресса повторная обработка
                    // сообщения будет пропущена.
                    tx.executeQueryWithCommit("""
                                        DECLARE $partition_id AS Int64;
                                        DECLARE $last_offset AS Int64;
                                        UPSERT INTO file_progress(partition_id, last_offset) VALUES ($partition_id, $last_offset);
                                    """,
                            Params.of(
                                    "$partition_id", PrimitiveValue.newInt64(partitionId),
                                    "$last_offset", PrimitiveValue.newInt64(message.getOffset())
                            )
                    );

                    LOGGER.info("Save: name {}, line {}, length {}", name, lineNumber, length);

                    // Для проверки правильной работы устанавливаем на следующей строке breakpoint c Condition: message.getOffset()==2
                    // В логе получаем при первом запуске строки save с line 1, 2 и 3
                    // После прерывания на breakpoint и повторного запуска получаем в логе остальные строки save с line 4 по 20
                    // Что означает обработку без повторений.

                    message.commit().join();

                    return CompletableFuture.completedFuture(Status.SUCCESS);
                }).join().expectSuccess();

            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }
        }

        LOGGER.info("Stopped read worker!");
    }

    private static void printTableFile(QueryServiceHelper queryServiceHelper) {
        // Выводим информацию об обработанных строках
        var queryReader = queryServiceHelper.executeQuery(
                "SELECT name, line, length FROM file;", TxMode.SERIALIZABLE_RW, Params.empty());

        for (ResultSetReader resultSet : queryReader) {
            while (resultSet.next()) {
                LOGGER.info(
                        "name: " + resultSet.getColumn(0).getText() +
                                ", line: " + resultSet.getColumn(1).getInt64() +
                                ", length: " + resultSet.getColumn(2).getInt64()
                );
            }
        }
    }

    private static void createSchema(QueryServiceHelper queryServiceHelper) {
        // Создаем таблицы для хранения информации о файле и прогрессе обработки
        queryServiceHelper.executeQuery("""
                CREATE TABLE IF NOT EXISTS file (
                    name Text NOT NULL,
                    line Int64 NOT NULL,
                    length Int64 NOT NULL,
                    PRIMARY KEY (name, line)
                );

                -- Таблица для хранения прогресса обработки партиции
                -- используется для того, чтобы не повторять обработку сообщений
                -- при перезапуске процесса.
                CREATE TABLE IF NOT EXISTS file_progress (
                    partition_id Int64 NOT NULL,
                    last_offset Int64 NOT NULL,
                    PRIMARY KEY (partition_id)
                );
                                
                CREATE TABLE IF NOT EXISTS write_file_progress (
                    name Text NOT NULL,
                    line_num Int64 NOT NULL,
                    PRIMARY KEY (name)
                );
                                                    
                CREATE TOPIC IF NOT EXISTS file_topic (
                    CONSUMER file_consumer
                ) WITH(
                    auto_partitioning_strategy='scale_up',
                    min_active_partitions=2,
                    max_active_partitions=5,
                    partition_write_speed_bytes_per_second=5000000
                );
                """);
    }

    private static void dropSchema(QueryServiceHelper queryServiceHelper) {
        queryServiceHelper.executeQuery("""
                DROP TABLE IF EXISTS file;
                DROP TABLE IF EXISTS file_progress;
                DROP TABLE IF EXISTS write_file_progress;
                DROP TOPIC IF EXISTS file_topic;
                """
        );
    }
}
