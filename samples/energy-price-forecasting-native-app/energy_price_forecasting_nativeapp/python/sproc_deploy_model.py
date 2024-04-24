from cachetools import cached
import pandas as pd
import snowflake.snowpark
import snowflake.snowpark.types as T
from snowflake.snowpark.files import SnowflakeFile


@cached(cache={})
def load_model(file_path: str) -> object:
    from joblib import load
    with SnowflakeFile.open(file_path, 'rb') as f:
        model = load(f)
    return model


# This local Python-function will be registered as a Stored Procedure and runs in Snowflake
def main(sp_session: snowflake.snowpark.Session, 
            training_table: str, file_path: str, target_table: str, append_mode: str, app_db: str, app_sch: str) -> str:
    data = sp_session.table("{0}.{1}".format(app_db, training_table)).to_pandas()
    predict_data=data.drop(['DATETIME','RTPRICE_TOMM'],axis=1)
    scoped_url = sp_session.sql("SELECT BUILD_SCOPED_FILE_URL(@{0}.{2}.ML_MODELS,'/model/{1}');".format(app_db, file_path, app_sch)).collect()[0][0]
    model = load_model(scoped_url)
    scored_data = pd.Series(model.predict(predict_data))
    data['PREDICTION'] = scored_data.values
    if append_mode == 'True' or append_mode == 'TRUE':
        sp_session.write_pandas(data, target_table,database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    else:
        sp_session.write_pandas(data, target_table,database=app_db, schema=app_sch, auto_create_table=False, overwrite=False)
    return 'Success'
