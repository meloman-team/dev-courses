package tech.ydb.app;

public record IssueLink (
        Issue main, // Главная задача
        Issue link // Связанная задача
) {
}
