package tech.ydb.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;

import java.time.Duration;
import java.util.List;

/**
 * @author Kirill Kurdyukov
 */
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
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

                var firstIssue = issueYdbRepository.addIssue("Ticket 1");
                issueYdbRepository.addIssue("Ticket 2");
                issueYdbRepository.addIssue("Ticket 3");

                for (var issue : issueYdbRepository.findAll()) {
                    LOGGER.info("Issue: {}", issue);
                }

                LOGGER.info("First issue: {}", issueYdbRepository.findById(firstIssue.id()));

                String prefix = String.valueOf(firstIssue.id()).substring(0,1);
                List<Issue> byPrefixId = issueYdbRepository.findByPrefixId(prefix);
                if(byPrefixId == null) {
                    LOGGER.debug("Not found by prefix \"{}\"", prefix);
                } else {
                    int i = 0;
                    LOGGER.debug("Found {} records by prefix \"{}\"", byPrefixId.size(), prefix);
                    for (Issue issue : byPrefixId) {
                        i++;
                        LOGGER.debug("[{}] {}", i, issue);
                    }
                }
            }
        }
    }
}
