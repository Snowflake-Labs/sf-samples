const snowflake = require('./spcs_helpers/connection.js')

// Create Snowflake connection pool with default min/max
connectionPool = snowflake.getPool({})

const sql = `
    SELECT
        current_account() AS account,
        current_database() AS database,
        current_schema() AS schema
`
connectionPool.use(async (connection) => {
    const statement = await connection.execute( {
        sqlText: sql,
        complete: function(err, stmt, rows) {
            if (err) {
                console.error("Failed to execute query: " + err.message)
                console.log("ERROR")
            }
            else {
                if ((process.env.SNOWFLAKE_ACCOUNT.toUpperCase() == rows[0].ACCOUNT) 
                &&  (process.env.SNOWFLAKE_DATABASE.toUpperCase() == rows[0].DATABASE)
                &&  (process.env.SNOWFLAKE_SCHEMA.toUpperCase() == rows[0].SCHEMA)
                )
                    console.log("\nSUCCESS")
                else
                    console.log("\nFAILED")
            }
        }
    })
})
