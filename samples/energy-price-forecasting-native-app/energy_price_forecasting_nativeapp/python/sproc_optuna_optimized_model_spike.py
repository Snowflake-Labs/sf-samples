import snowflake.snowpark

def sproc_optuna_optimized_model(sp_session: snowflake.snowpark.Session, 
                                 table_name: str, 
                                 model_name: str,
                                 n_trials: int, app_db: str, app_sch: str) -> str:
    
    
    import optuna
    import sklearn.ensemble
    import xgboost
    import numpy as np
    from sklearn.metrics import average_precision_score
    import pandas as pd
    from sklearn.model_selection import TimeSeriesSplit
    
    # Loading data into pandas dataframe
    train_val = sp_session.table("{0}.{1}.{2}".format(app_db, app_sch,table_name)).to_pandas()
    train_val=train_val[['2023' not in i for i in train_val.DATETIME]]
    
    tscv = TimeSeriesSplit()
    
    X=train_val.drop(['RTPRICE_TOMM','DATETIME'], axis=1)
    y=train_val['RTPRICE_TOMM'] 
    
    
    def objective(trial):
        xgb_n_estimators = trial.suggest_int("xgb_n_estimators", 5, 100, log=True)
        xgb_max_depth = trial.suggest_int("xgb_max_depth", 2, 32, log=True)
        xgb_subsample = trial.suggest_float("xgb_subsample", 0.01, 1, log=True)
        xgb_colsample_bytree = trial.suggest_float("xgb_colsample_bytree", 0.01, 1, log=True)
        xgb_min_child_weight = trial.suggest_float("xgb_min_child_weight", 1, 75, log=True)
        xgb_learning_rate = trial.suggest_float("xgb_learning_rate", 0.01, .7, log=True)
        classifier_obj = xgboost.XGBClassifier(
            n_estimators= xgb_n_estimators, max_depth=xgb_max_depth, subsample=xgb_subsample, 
            colsample_bytree=xgb_colsample_bytree, min_child_weight=xgb_min_child_weight, learning_rate=xgb_learning_rate
        )
        
        score=[]
        for train_index, valid_index in tscv.split(train_val):
            train, valid = train_val.iloc[train_index], train_val.iloc[valid_index]
            classifier_obj.fit(train.drop(['RTPRICE_TOMM','DATETIME'], axis=1),train['RTPRICE_TOMM'] )
            predictions=classifier_obj.predict(valid.drop(['RTPRICE_TOMM','DATETIME'], axis=1))
            score.append(average_precision_score(predictions,valid.RTPRICE_TOMM))                                  
        score = sum(score)/len(score)
        trial.set_user_attr(key="best_booster", value=classifier_obj)
        return score
    
    # Callback to get best model
    def callback(study, trial):
        if study.best_trial.number == trial.number:
            study.set_user_attr(key="best_booster", value=trial.user_attrs["best_booster"])

    # Start Optimizing
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials, callbacks=[callback])
    
    # Fit best model on data
    best_model=study.user_attrs["best_booster"]
    best_model.fit(X.values, y.values)
    
    # Save model as file and upload to Snowflake stage
    from joblib import dump
    dump(best_model, '/tmp/'+model_name)
    sp_session.file.put('/tmp/'+model_name, '@{0}.{1}.ML_SPIKE_MODELS'.format(app_db, app_sch), auto_compress=False, overwrite=True)
    output = study.best_trial.params
    df_out = pd.DataFrame(output.items(), columns=['Parameters', 'Value'])
    sp_session.write_pandas(df_out, 'HYPER_PARAMETER_OUTPUT_SPIKE',database=app_db, schema=app_sch, auto_create_table=True, overwrite=True)
    return 'Success'
