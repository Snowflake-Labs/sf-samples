from snowflake import snowpark

def generate_data(session: snowpark.Session, table_name: str, num_rows: int, overwrite: bool = False) -> list[snowpark.Row]:
    query = f"""
        CREATE{" OR REPLACE" if overwrite else ""} TABLE{"" if overwrite else " IF NOT EXISTS"} {table_name} AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as application_id,
            ROUND(NORMAL(40, 10, RANDOM())) as age,
            ROUND(NORMAL(65000, 20000, RANDOM())) as income,
            ROUND(NORMAL(680, 50, RANDOM())) as credit_score,
            ROUND(NORMAL(5, 2, RANDOM())) as employment_length,
            ROUND(NORMAL(25000, 8000, RANDOM())) as loan_amount,
            ROUND(NORMAL(35, 10, RANDOM()), 2) as debt_to_income,
            ROUND(NORMAL(5, 2, RANDOM())) as number_of_credit_lines,
            GREATEST(0, ROUND(NORMAL(1, 1, RANDOM()))) as previous_defaults,
            ARRAY_CONSTRUCT(
                'home_improvement', 'debt_consolidation', 'business', 'education',
                'major_purchase', 'medical', 'vehicle', 'other'
            )[UNIFORM(1, 8, RANDOM())] as loan_purpose,
            RANDOM() < 0.15 as is_default,
            TIMEADD("MINUTE", UNIFORM(-525600, 0, RANDOM()), CURRENT_TIMESTAMP()) as created_at
        FROM TABLE(GENERATOR(rowcount => {num_rows}))
        ORDER BY created_at;
    """
    return session.sql(query).collect()

if __name__ == "__main__":
    import argparse
    from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table_name", default="loan_applications", help="Name of input data table"
    )
    parser.add_argument(
        "-n", "--num_rows", type=int, default=10000, help="Number of rows to generate"
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite table if it already exists"
    )
    args = parser.parse_args()

    session = snowpark.Session.builder.configs(SnowflakeLoginOptions()).create()
    generate_data(session, **vars(args))