import snowflake.snowpark
def sproc_final_model(sp_session: snowflake.snowpark.Session, 
                                 training_table: str, 
                                 split: int,
                                 app_db: str, app_sch: str) -> str:
    
    import xgboost
    import numpy as np
    from sklearn.metrics import mean_squared_error
    import pandas as pd
    from sklearn.metrics import mean_absolute_error
    
    # Loading data into pandas dataframe
    data = sp_session.table("{0}.{1}.{2}".format(app_db, app_sch,training_table)).to_pandas()
    datetime=data.DATETIME[split:split+7*24].reset_index(drop=True)
    data=data.drop('DATETIME',axis=1)
    hyper_df = sp_session.sql('select * from {0}.{1}.hyper_parameter_output;'.format(app_db, app_sch)).to_pandas()
    regressor_obj = xgboost.XGBRegressor(
        n_estimators= int(hyper_df.loc[hyper_df['Parameters'] == 'xgb_n_estimators', 'Value'].iloc[0]),
        max_depth=int(hyper_df.loc[hyper_df['Parameters'] == 'xgb_max_depth', 'Value'].iloc[0]),
        subsample=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_subsample', 'Value'].iloc[0]),
        colsample_bytree=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_colsample_bytree', 'Value'].iloc[0]),
        min_child_weight=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_min_child_weight', 'Value'].iloc[0]),
        learning_rate=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_learning_rate', 'Value'].iloc[0])
    )
    score=[]
    #training and getting scores through Rolling cross val
    train, valid = data.iloc[:split], data.iloc[split:split+7*24]
    regressor_obj.fit(train.drop('RTPRICE_TOMM', axis=1),train['RTPRICE_TOMM'])
    #make predictions
    predictions=regressor_obj.predict(valid.drop('RTPRICE_TOMM', axis=1))
    predictions=[0 if i<0 else i for i in predictions]
    #MSE and MAE calculations
    mse=mean_squared_error(predictions,valid.RTPRICE_TOMM)
    mae=mean_absolute_error(predictions,valid.RTPRICE_TOMM)
    #organize results and MSE/MAE into dataframe
    preds=pd.DataFrame({'Predictions': list(predictions), 'Ground_Truth': list(valid.RTPRICE_TOMM), 'Datetime': list(datetime), 'MAE': list(np.repeat(mae,7*24)), 'MSE': list(np.repeat(mse,7*24))})
    try:
        pred_table=sp_session.table('{0}.{1}.PREDICTIONS'.format(app_db, app_sch)).to_pandas()
        save_it=pd.concat([pred_table, preds]).drop_duplicates(['Datetime'])
        sp_session.write_pandas(save_it, 'PREDICTIONS', database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    except:
        sp_session.write_pandas(preds, 'PREDICTIONS', database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    #add metrics to scores
    score.append(mean_squared_error(predictions,valid.RTPRICE_TOMM)) 
    score.append(mean_absolute_error(predictions,valid.RTPRICE_TOMM))
    #save model at each retraining
    from joblib import dump
    dump(regressor_obj, '/tmp/'+'forecast_'+f'{datetime[0]}'+'.sav')
    sp_session.file.put('/tmp/'+'forecast_'+f'{datetime[0]}'+'.sav', '@{0}.{1}.ML_MODELS/model/'.format(app_db, app_sch), auto_compress=False, overwrite=True)
    return 'Success'