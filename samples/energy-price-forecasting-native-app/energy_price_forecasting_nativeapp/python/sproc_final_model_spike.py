import snowflake.snowpark
def sproc_final_model1(sp_session: snowflake.snowpark.Session, 
                                 training_table: str, 
                                 split: int,
                                 app_db:str, app_sch:str) -> str:

    import xgboost
    import numpy as np
    import pandas as pd
    from sklearn.metrics import (
    roc_auc_score, 
    average_precision_score, 
    precision_score,
    recall_score,
    precision_recall_curve,
    roc_curve
        
    )
    
    # Loading data into pandas dataframe
    data = sp_session.table("{0}.{1}.{2}".format(app_db, app_sch,training_table)).to_pandas()
    datetime=data.DATETIME[split:split+7*24].reset_index(drop=True)
    data=data.drop('DATETIME',axis=1)
    hyper_df = sp_session.sql('select * from {0}.{1}.HYPER_PARAMETER_OUTPUT_SPIKE;'.format(app_db, app_sch)).to_pandas()
    classifier_obj = xgboost.XGBClassifier(
        n_estimators= int(hyper_df.loc[hyper_df['Parameters'] == 'xgb_n_estimators', 'Value'].iloc[0]),
        max_depth=int(hyper_df.loc[hyper_df['Parameters'] == 'xgb_max_depth', 'Value'].iloc[0]),
        subsample=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_subsample', 'Value'].iloc[0]),
        colsample_bytree=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_colsample_bytree', 'Value'].iloc[0]),
        min_child_weight=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_min_child_weight', 'Value'].iloc[0]),
        learning_rate=float(hyper_df.loc[hyper_df['Parameters'] == 'xgb_learning_rate', 'Value'].iloc[0])
    )

    # data from threshold analysis
    y='RTPRICE_TOMM'
    scored_sdf = sp_session.sql("SELECT * FROM {0}.{1}.FORECASTED_RESULT;".format(app_db, app_sch)).to_pandas()
    scored_sdf = scored_sdf[['PREDICTION', y]]
    y_pred = scored_sdf['PREDICTION']
    y_true = scored_sdf[y]
    fpr, tpr, t = roc_curve(y_true, y_pred)
    auc = roc_auc_score(y_true, y_pred)
    p, r, c = precision_recall_curve(y_true, y_pred)
    f1 = 2*p*r/(p+r)
    t = c[f1.argmax()]

    # Prediction
    score=[]
    train, valid = data.iloc[:split], data.iloc[split:split+7*24]
    classifier_obj.fit(train.drop('RTPRICE_TOMM', axis=1),train['RTPRICE_TOMM'])
    predictions=classifier_obj.predict_proba(valid.drop('RTPRICE_TOMM', axis=1))[:,1]
    predictions=predictions>t
    #try except to define AUC score as -1 if it is undefined
    try:
        auc = roc_auc_score(valid.RTPRICE_TOMM, predictions)
    except:
        auc = -1
    #Define the performance metrics
    ap = average_precision_score(valid.RTPRICE_TOMM, predictions)
    r=recall_score(valid.RTPRICE_TOMM, predictions)
    p=precision_score(valid.RTPRICE_TOMM, predictions)
    preds=pd.DataFrame({'Predictions': list(predictions), 'Ground_Truth': list(valid.RTPRICE_TOMM), 'Datetime': list(datetime), 
                        'Precision': list(np.repeat(p,7*24)), 'Recall': list(np.repeat(r,7*24)), 'AUC': list(np.repeat(auc,7*24)),
                       'AP':list(np.repeat(ap,7*24))})
    try:
        pred_table=sp_session.table('{0}.{1}.PREDICTIONS1'.format(app_db, app_sch)).to_pandas()
        save_it=pd.concat([pred_table, preds]).drop_duplicates(['Datetime'])
        sp_session.write_pandas(save_it, 'PREDICTIONS1',database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    except:
        sp_session.write_pandas(preds, 'PREDICTIONS1',database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    score.append(ap) 
    score.append(p)
    score.append(r) 
    score.append(auc)
    
    from joblib import dump
    dump(classifier_obj, '/tmp/'+'forecast_'+f'{datetime[0]}'+'.sav')
    sp_session.file.put('/tmp/'+'forecast_'+f'{datetime[0]}'+'.sav', '@{0}.{1}.ML_SPIKE_MODELS/spikemodel/'.format(app_db, app_sch), auto_compress=False, overwrite=True)
    return 'Success'