# Define a simple scoring function
from cachetools import cached
import pandas as pd
import snowflake.snowpark
import snowflake.snowpark.types as T
from snowflake.snowpark.files import SnowflakeFile


def main(sp_session: snowflake.snowpark.Session, 
                                 training_table: str, app_db: str, app_sch:str) -> str:
    @cached(cache={})
    def load_model(file_path: str) -> object:
        from joblib import load
        with SnowflakeFile.open(file_path, 'rb') as f:
            model = load(f)
        return model

    import pandas as pd
    import xgboost
    model_name = 'optuna_model_s.sav'
    scoped_url = sp_session.sql("SELECT BUILD_SCOPED_FILE_URL(@{1}.{2}.ML_SPIKE_MODELS,'{0}');".format(model_name, app_db, app_sch)).collect()[0][0]
    model = load_model(scoped_url)
    
    test = sp_session.sql("SELECT * FROM {1}.{2}.{0}".format(training_table, app_db, app_sch)).to_pandas()
    test = test[['2021' in i for i in test.DATETIME]]
    sp_session.write_pandas(test, 'TEST', database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    test=sp_session.sql("SELECT * FROM {0}.{1}.{2}".format(app_db, app_sch, 'TEST')).to_pandas()
    predict_data=sp_session.table("{0}.{1}.{2}".format(app_db, app_sch, 'TEST')).drop(['RTPRICE_TOMM','DATETIME'])
    predict_data = predict_data.to_pandas()
    scored_data = pd.Series(model.predict_proba(predict_data)[:,1])
    test['PREDICTION'] = scored_data.values
    sp_session.write_pandas(test, 'FORECASTED_RESULT', database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    return 'Success'