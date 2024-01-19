package com.snowflake;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws SQLException, IOException {

        var connection = ConnectionFactory.GetConnection();
        Statement statement = connection.createStatement();
        String sql = "SELECT current_account() AS account, current_database() AS database, current_schema() AS schema";
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            String account = resultSet.getString(1);
            String database = resultSet.getString(2);
            String schema = resultSet.getString(3);
            if (account.equals(System.getenv("SNOWFLAKE_ACCOUNT")) &&
                    database.equals(System.getenv("SNOWFLAKE_DATABASE")) &&
                    schema.equals(System.getenv("SNOWFLAKE_SCHEMA")))
            {
                System.out.print("SUCCESS");
                System.exit(0);
            }
        }
        System.out.print("FAILED");
        System.exit(1);
    }
}