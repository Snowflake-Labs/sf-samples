#!/opt/conda/bin/python3
import argparse
import logging
import os
import sys
import optuna
import sklearn.ensemble
import sklearn.model_selection
import sklearn.metrics
import xgboost
import numpy as np
import pandas as pd
import psutil


from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import *

# Environment variables below will be automatically populated by Snowflake.
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Custom environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")


def get_arg_parser():
    """
    Input argument list.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--training_table", required=True, help="table with training data")
    parser.add_argument("--training_frac", required=True, help="fraction of data to use for training")
    parser.add_argument("--hpo_frac", required=True,  help="fraction of data to use for HPO")
    parser.add_argument("--n_trials", required=True, help="number of HPO trials")
    parser.add_argument("--feature_cols", required=True, help="Independent Features")
    parser.add_argument("--target_col", required=True, help="Dependent Feature (Classification Target)")
    parser.add_argument("--model_name", required=True, help="name for saving model")
    return parser


def get_logger():
    """
    Get a logger for local logging.
    """
    logger = logging.getLogger("job-tutorial")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_login_token():
    """
    Read the login token supplied automatically by Snowflake. These tokens
    are short lived and should always be read right before creating any new connection.
    """
    with open("/snowflake/session/token", "r") as f:
        return f.read()


def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    if os.path.exists("/snowflake/session/token"):
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": get_login_token(),
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "user": SNOWFLAKE_USER,
            "password": SNOWFLAKE_PASSWORD,
            "role": SNOWFLAKE_ROLE,
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }



def data_prep(df, feature_cols, target_col, training_frac, logger):
    """
    prep data for training: extract embeddings, transform categorical features
    """
    # Select feature columns
    X = df[feature_cols]
    # Extract embeddings and convert into dataframe
    logger.info(
        f"\tExtracting Embeddings. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    embedding_array = np.stack(X.EMBEDDINGS.apply(lambda x: np.array(eval(x))))
    logger.info(
        f"\tConverting Embeddings to pd.DataFrame. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    embeddings_only = pd.DataFrame(embedding_array, columns=['EMBEDDING_'+str(i)for i in range(512)])

    logger.info(
        f"\tConcating Embeddings. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    # combine embeddings and non-embedding features into single dataframe
    X = pd.concat([X.drop('EMBEDDINGS', axis=1), embeddings_only], axis=1)
    logger.info(
        f"\tRAM memory % used: {psutil.virtual_memory()[2]}"
    )
    # convert gender from string to binary feature
    X.GENDER = X.GENDER.apply(lambda x: x=='Female').astype(int)
    # 1-hot encode profession 
    profession_columns = [
        'PROFESSION_ARTIST','PROFESSION_DOCTOR','PROFESSION_ENGINEER',
        'PROFESSION_ENTERTAINMENT','PROFESSION_EXECUTIVE','PROFESSION_HEALTHCARE',
        'PROFESSION_HOMEMAKER','PROFESSION_LAWYER','PROFESSION_MARKETING'
    ]
    for c in profession_columns:
        X[c] = (X.PROFESSION == c[11:]).astype(int)
    X = X.drop('PROFESSION', axis=1)
    y = df[target_col]
    # create training subsample
    train_sample = X.sample(frac=training_frac).index
    return X, y, train_sample


def run_optuna(X, y, n_trials):
    """
    run optuna HPO job
    """
    def objective(trial):
        """
        optuna optimizer objective function
        trains rf or xgb boost with selected hyper parameters
        performance measured by roc AUC
        """
        classifier_name = trial.suggest_categorical("classifier", ["RandomForest", "XGBoost"])
        if classifier_name == "RandomForest":
            rf_max_depth = trial.suggest_int("rf_max_depth", 2, 32, log=True)
            rf_n_estimators = trial.suggest_int("rf_n_estimators", 5, 100, log=True)
            rf_max_samples = trial.suggest_float("rf_max_samples", 0.01, 1, log=True)
            classifier_obj = sklearn.ensemble.RandomForestClassifier(n_jobs=-1,
                max_depth=rf_max_depth, n_estimators=rf_n_estimators, max_samples=rf_max_samples
            )
        else:
            xgb_n_estimators = trial.suggest_int("xgb_n_estimators", 5, 100, log=True)
            xgb_max_depth = trial.suggest_int("xgb_max_depth", 2, 32, log=True)
            xgb_subsample = trial.suggest_float("xgb_subsample", 0.01, 1, log=True)
            classifier_obj = xgboost.XGBClassifier(n_jobs=-1,
                n_estimators=xgb_n_estimators, max_depth=xgb_max_depth, subsample=xgb_subsample
            )
        # model trained on and predicts on all 4 clickstream events)
        y_pred = sklearn.model_selection.cross_val_predict(classifier_obj, X, y, cv=3, n_jobs=-1, method='predict_proba')
        y_binary = y == 3
        # convert class scores to combined purchase score, we evaluate purchase vs. no purchase
        weights = np.array([0,.05,0.075,0.875])
        fpr, tpr, thresholds = sklearn.metrics.roc_curve(y_binary, (weights*y_pred).sum(axis=1))
        score = sklearn.metrics.auc(fpr, tpr)
        trial.set_user_attr(key="best_booster", value=classifier_obj)
        return score

    # Callback to get best model
    def callback(study, trial):
        if study.best_trial.number == trial.number:
            study.set_user_attr(key="best_booster", value=trial.user_attrs["best_booster"])
    # create HPO study and set to maximize objective (roc AUC)          
    study = optuna.create_study(direction="maximize")
    # run HPO study
    study.optimize(objective, n_trials=n_trials, callbacks=[callback])
    return study.user_attrs["best_booster"], study.best_trial.params
        
    
def run_job():
    logger = get_logger()
    logger.info("Job started")

    # Parse input arguments
    args = get_arg_parser().parse_args()
    training_table = args.training_table
    training_frac = float(args.training_frac)
    hpo_frac = float(args.hpo_frac)
    n_trials = int(args.n_trials)
    feature_cols = eval(args.feature_cols)
    target_col = args.target_col
    model_name = args.model_name
            
    # create snowflake session to access training data
    with Session.builder.configs(get_connection_params()).create() as session:
        # connecting stuff
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        logger.info(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )
        # 1. load data
        logger.info(
            f"loading data from {training_table}. RAM memory % used: {psutil.virtual_memory()[2]}"
        )
        df = session.table(training_table).sample(frac=training_frac).to_pandas()
        logger.info(
            f"loaded {len(df)} rows of training data. RAM memory % used: {psutil.virtual_memory()[2]}"
        )
    # following steps do not need the snowflake session
    # 2. data prep
    X, y, train_sample = data_prep(df, feature_cols, target_col, hpo_frac, logger)
    logger.info(
        f"preprocessed data, {len(train_sample)} rows will be used for HPO. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    # 3. Start Optimizing
    logger.info(
        f"Running HPO..."
    )
    best_model, best_params = run_optuna(X.iloc[train_sample], y.iloc[train_sample], n_trials)
    logger.info(
        f"Best model found had the following parameters:\n{best_params}\nRAM memory % used: {psutil.virtual_memory()[2]}"
    )

    # 4. Fit best model on full dataset
    logger.info(
        f"Fitting Best Model. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    best_model.fit(X.values, y.values)
    # 5. Save model as file and upload to Snowflake stage
    logger.info(
        f"Saving Best Model. RAM memory % used: {psutil.virtual_memory()[2]}"
    )
    # create new snowflake session, as the prior session would likely expire before this step
    with Session.builder.configs(get_connection_params()).create() as session:
        # connecting stuff
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        logger.info(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )
        from joblib import dump
        dump(best_model, '/tmp/'+model_name)
        session.file.put('/tmp/'+model_name, '@MODEL_STG', auto_compress=False, overwrite=True)
    logger.info("Job finished")
    

    
if __name__ == "__main__":
    run_job()

