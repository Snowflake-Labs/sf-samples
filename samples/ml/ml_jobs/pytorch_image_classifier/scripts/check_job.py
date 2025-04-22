import argparse
import snowflake.ml.jobs as jobs

from snowflake.snowpark import Session
from snowflake.ml.utils.connection_params import SnowflakeLoginOptions

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job_id", type=str, help="Job ID to check")
    parser.add_argument("--show-logs", action="store_true", help="Show job logs")
    parser.add_argument("--block", action="store_true", help="Block until job completes")
    parser.add_argument("-c", "--snowflake-config", type=str, required=False)
    args = parser.parse_args()

    session = Session.builder.configs(SnowflakeLoginOptions(args.snowflake_config)).create()

    job = jobs.get_job(args.job_id, session=session)
    if args.block:
        job.wait()
    print("Job status:", job.status)
    if args.show_logs:
        job.show_logs()

if __name__ == '__main__':
    main()