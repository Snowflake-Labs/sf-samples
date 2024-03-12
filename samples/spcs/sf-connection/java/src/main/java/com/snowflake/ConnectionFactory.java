package com.snowflake;
import java.nio.file.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.IOException;
import java.util.Properties;

public class ConnectionFactory {

    private static final String TOKENPATH = "/snowflake/session/token";

    public static java.sql.Connection GetConnection() throws IOException, SQLException {
        String account = System.getenv("SNOWFLAKE_ACCOUNT");
        String database = System.getenv("SNOWFLAKE_DATABASE");
        String schema = System.getenv("SNOWFLAKE_SCHEMA");
        String host = System.getenv("SNOWFLAKE_HOST");
        Path tokenFile = Paths.get(TOKENPATH);
        if (Files.exists(tokenFile)) {
            String token = new String(Files.readAllBytes(tokenFile));
            Properties props = new Properties();
            props.put("authenticator", "oauth");
            props.put("account", account);
            props.put("token", token);
            props.put("db", database);
            props.put("schema", schema);
            props.put("tracing", "INFO");
            String url = String.format("jdbc:snowflake://%s/", host);
            return DriverManager.getConnection(url, props);
        } else {
            Properties props = new Properties();
            props.put("account", account);
            props.put("user", System.getenv("SNOWFLAKE_USER"));
            props.put("password", System.getenv("SNOWFLAKE_PASSWORD"));
            props.put("db", database);
            props.put("schema", schema);
            props.put("tracing", "INFO");
            if (host.isEmpty()) {
                String url = String.format("jdbc:snowflake://%s.snowflakecomputing.com/", account);
                return DriverManager.getConnection(url, props);
            }
            String url = String.format("jdbc:snowflake://%s", host);
            return DriverManager.getConnection(url, props);
        }
    }

}
