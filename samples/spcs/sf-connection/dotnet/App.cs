using System;
using System.Data;
using System.Data.Common;
using System.IO;

using Snowflake.Data.Core;
using Snowflake.Data.Client;

class App
{
    const string TOKENPATH = "/snowflake/session/token";

    static string getConnectionString(){
        string? account = Environment.GetEnvironmentVariable("SNOWFLAKE_ACCOUNT");
        string? database = Environment.GetEnvironmentVariable("SNOWFLAKE_DATABASE");
        string? schema = Environment.GetEnvironmentVariable("SNOWFLAKE_SCHEMA");
        string? host = Environment.GetEnvironmentVariable("SNOWFLAKE_HOST");
        if (File.Exists(TOKENPATH)) {
            // automatically set by env
            string token = File.ReadAllText(TOKENPATH);
            return $"account={account};authenticator=oauth;token={token};db={database};schema={schema};host={host}";
        } else {
            // basic auth, variables must be set by user
            string? user = Environment.GetEnvironmentVariable("SNOWFLAKE_USER");
            string? password = Environment.GetEnvironmentVariable("SNOWFLAKE_PASSWORD");
            return $"account={account};user={user};password={password};db={database};schema={schema};host={host}";
        }
    }

    static int Main()
    {
        try
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = getConnectionString();
                conn.Open();
                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT current_account() AS account, current_database() AS database, current_schema() AS schema";
                    IDataReader reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        string account, database, schema;
                        account = reader.GetString(0);
                        database = reader.GetString(1);
                        schema = reader.GetString(2);
                        if (account.Equals(Environment.GetEnvironmentVariable("SNOWFLAKE_ACCOUNT")) &&
                            database.Equals(Environment.GetEnvironmentVariable("SNOWFLAKE_DATABASE")) &&
                            schema.Equals(Environment.GetEnvironmentVariable("SNOWFLAKE_SCHEMA"))
                        )
                        {
                            Console.WriteLine("SUCCESS");
                            conn.Close();
                            return 0;
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
        Console.WriteLine("FAILED");
        return 1;
    }
}