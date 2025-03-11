# Import python packages
import streamlit as st
# UNCOMMENT FOR NATIVE APP STARTS ##
from snowflake.snowpark.context import get_active_session
# UNCOMMENT FOR NATIVE APP ENDS ##
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
import seaborn as sns
from tqdm import tqdm
import altair as at
import datetime
from sklearn.metrics import (
    roc_auc_score, 
    average_precision_score, 
    precision_score,
    recall_score   
    )
from sklearn.metrics import (
    ConfusionMatrixDisplay, 
    roc_auc_score, 
    average_precision_score, 
    precision_recall_curve, 
    roc_curve,
    confusion_matrix, accuracy_score, classification_report
)

from datetime import date
st.set_page_config(layout="wide")

## defining functions used in the app
@st.cache_data(ttl=900)
def get_distinct_dates_predicted(_sp_session, app_db, app_sch):
    """ Get distinct dates post the prediction
    """ 
    sql_stmt = "select distinct concat(DATE_PART('YEAR',to_timestamp(\"Datetime\")),'-', LPAD(DATE_PART('MM',to_timestamp(\"Datetime\")),2,0),'-', LPAD(DATE_PART('DD',to_timestamp(\"Datetime\")),2,0)) as DATES from {0}.{1}.predictions order by DATES desc;".format(app_db, app_sch)
    df = _sp_session.sql(sql_stmt).to_pandas()
    return df['DATES'].values.tolist()


@st.cache_data(ttl=900)
def get_predictions_results(_sp_session, app_db, app_sh, table_name):
    """ Get predictions table data into a dataframe
    """ 
    return _sp_session.table('{0}.{1}.{2}'.format(app_db, app_sh, table_name)).to_pandas()


@st.cache_data(ttl=100)
def get_tables_list(_sp_session, db_name):
    """ Get the tables list for auto-displaying. 
    """  
    #_sp_session.use_database('{0}'.format(db_name))
    get_tables_sql_stmt = "select concat(table_schema, '.', table_name) as TABLE_NAME  from information_schema.tables  where table_schema not in ('INFORMATION_SCHEMA') order by table_schema, table_name;"  
    df = _sp_session.sql(get_tables_sql_stmt).to_pandas()

    return df['TABLE_NAME'].values.tolist()


#@st.cache_data(ttl=900)
def get_models_list(_sp_session, app_db, stage_name, app_sch):
    """ Get the models list from stage for auto-displaying. 
    """    
    #_sp_session.use_database('{0}'.format(app_db))
    show_command = "list @{0}.{1}.{2} PATTERN='.*forecast_.*';".format(app_db, app_sch, stage_name)
    _sp_session.sql(show_command).collect()
    get_models_sql_stmt = 'SELECT split_part("name", \'/\',  3) as MODELS FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) order by "last_modified" desc;'
    df = _sp_session.sql(get_models_sql_stmt).to_pandas()

    return df['MODELS'].values.tolist()

@st.cache_data(ttl=900)
def get_warehouses_list(_sp_session):
    """ Get the tables list for auto-displaying. 
    """  
    show_command = "SHOW WAREHOUSES in account;"  
    _sp_session.sql(show_command).collect()
    get_wh_sql_stmt = 'SELECT "name" as WH_NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));'
    df = _sp_session.sql(get_wh_sql_stmt).to_pandas()

    return df['WH_NAME'].values.tolist()

@st.cache_data(ttl=900)
def get_column_list(_sp_session, source_table):
   try:
      df = _sp_session.sql("select column_name from information_schema.columns where table_schema ilike '{0}' and table_name ilike '{1}' \
                   order by ordinal_position;".format(source_table.split('.')[1], source_table.split('.')[2])).to_pandas()
   except Exception as e:
      df = _sp_session.sql("select column_name from information_schema.columns where table_schema ilike '{0}' and table_name ilike '{1}' \
                   order by ordinal_position;".format(source_table.split('.')[0], source_table.split('.')[1])).to_pandas()
   return df['COLUMN_NAME'].values.tolist()



def deploy_price_model_localst(sp_session, app_db, model_name, frequency, source_table, target_table, wh_name, fun_sch):
    """ Deploy the model that was selected as Dynamic Table
    ¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡ THIS WORKS ONLY ON LOCALLY HOSTED APP FOR NOW - DUE TO LIMITAIONS WITH NATIVEAPP !!!!!!!!!!!!!!!!!!!!!!!
    """
    sp_session.sql("CALL {0}.{1}.SPROC_REGISTER_UDF_WITH_MODEL('{2}', '{0}');".format(app_db, 
                                                                               fun_sch, 
                                                                               model_name)).collect()
    col_list = get_column_list(sp_session, source_table)
    col_str = ', '.join('"{0}"'.format(w) for w in col_list)
    col_without_price_date = ', '.join('"{0}"'.format(w) for w in col_list if w not in ('RTPRICE_TOMM', 'DATETIME'))
    dynamic_tbl_stmt = "CREATE OR REPLACE DYNAMIC TABLE {0} TARGET_LAG = '{1} minute' WAREHOUSE = {2} AS \
                        SELECT {3}, {4}.{5}.UDF_PREDICT({7}) as PREDICTION FROM {6}".format(target_table,
                                                                                            frequency,
                                                                                            wh_name,
                                                                                            col_str,
                                                                                            app_db, 
                                                                                            app_sch,
                                                                                            source_table,
                                                                                            col_without_price_date)
    sp_session.sql(dynamic_tbl_stmt).collect()
    return 'Success'



def deploy_price_model(sp_session, app_db, model_name, frequency, source_table, target_table, wh_name, append_mode, app_sch, fun_sch):
   """ Deploy the model that was selected as Streams and Tasks
   """
   if append_mode == 'True':
      task_stmt = "CALL {0}.{6}.SPROC_DEPLOY_MODEL('{1}', '{2}', '{3}', '{4}', '{0}', '{5}');".format(app_db, source_table, model_name, target_table, append_mode, app_sch, fun_sch)
      sp_session.sql(task_stmt).collect()
      return 'Initial Load'
   else:
      sp_session.sql("CREATE STREAM IF NOT EXISTS ENGY_FORECASTING_SOURCE_STREAM ON VIEW {0}.{1};".format(app_db, source_table)).collect()
      task_stmt = "CREATE TASK IF NOT EXISTS {2}.{7}.ENERGY_FORECASTING_INFERENCE_TASK  \
                        WAREHOUSE = {0}  \
                        SCHEDULE = '{1} MINUTE' \
                        WHEN \
                        SYSTEM$STREAM_HAS_DATA('ENGY_FORECASTING_SOURCE_STREAM') \
                        AS \
                        CALL {2}.{8}.SPROC_DEPLOY_MODEL('{3}',  \
                     '{4}', '{5}', '{6}', '{2}', '{7}');".format(wh_name, frequency, app_db, source_table, model_name, target_table, append_mode, app_sch, fun_sch)
      sp_session.sql(task_stmt).collect()
      sp_session.sql("ALTER TASK IF EXISTS ENERGY_FORECASTING_INFERENCE_TASK RESUME").collect()
      # sp_session.sql("CALL {0}.PYTHON_FUNCTIONS.udf_deploy_model();".format(app_db)).collect()
      # col_list = get_column_list(sp_session, source_table)
      # col_str = ', '.join('"{0}"'.format(w) for w in col_list)
      # col_without_price_date = ', '.join('"{0}"'.format(w) for w in col_list if w not in ('RTPRICE_TOMM', 'DATETIME'))
      # col_without_price_date = col_without_price_date + ", '@{0}.CRM.ML_MODELS/model/{1}'".format(app_db, model_name)
      # dynamic_tbl_stmt = "CREATE OR REPLACE DYNAMIC TABLE {0} TARGET_LAG = '{1} minute' WAREHOUSE = {2} AS \
      #                   SELECT {3}, {4}.{5}.udf_predict_price({7}) as PREDICTION FROM {6}".format(target_table,
      #                                                                                        frequency,
      #                                                                                        wh_name,
      #                                                                                        col_str,
      #                                                                                        app_db, 
      #                                                                                        'CRM',
      #                                                                                        source_table,
      #                                                                                        col_without_price_date)
      # sp_session.sql(dynamic_tbl_stmt).collect()
      return 'Dynamic Table Created'


@st.cache_data(ttl=900)
def perform_hyper_parameter_tuning(_sp_session, app_db, table_name, model_name, n_trials, app_sch, fun_sch):
    """ Perform Hyper Parameter Tuning and return the results in a dataframe
    """   
    sql_call_stmt = "call {4}.{3}.sproc_optuna_optimized_model('{0}','{1}',{2}, '{4}', '{5}')".format(table_name, model_name, n_trials, fun_sch, app_db, app_sch)
    out_str = _sp_session.sql(sql_call_stmt).collect()
    st.write(out_str[0][0])
    hyper_df = pd.DataFrame()
    if out_str[0][0] == 'Success':
        hyper_df = _sp_session.sql('select * from {0}.{1}.HYPER_PARAMETER_OUTPUT;'.format(app_db, app_sch)).to_pandas()
        return hyper_df
    

def deploy_spike_model(sp_session, app_db, model_name, frequency, source_table, target_table, wh_name, append_mode, fun_sch, app_sch):
    """ Deploy the model that was selected as Streams and Tasks
    """
    if append_mode == 'True':
      task_stmt = "CALL {5}.{4}.sproc_deploy_model_spike('{0}', '{1}', '{2}', '{3}', '{5}', '{6}');".format(source_table, model_name, 
                                                                                                            target_table, append_mode, 
                                                                                                            fun_sch, app_db, app_sch)
      sp_session.sql(task_stmt).collect()
      return 'Initial Load'
    else:
      sp_session.sql("CREATE STREAM IF NOT EXISTS ENGY_FORECASTING_SOURCE_STREAM_SPIKE ON VIEW {0}.{1};".format(app_db, source_table)).collect()
      task_stmt = "CREATE TASK IF NOT EXISTS {2}.{8}.ENERGY_SPIKE_FORECASTING_INFERENCE_TASK  \
                        WAREHOUSE = {0}  \
                        SCHEDULE = '{1} MINUTE' \
                        WHEN \
                        SYSTEM$STREAM_HAS_DATA('ENGY_FORECASTING_SOURCE_STREAM_SPIKE') \
                        AS \
                        CALL {2}.{7}.sproc_deploy_model_spike('{3}',  \
                     '{4}', '{5}', '{6}', '{2}', '{8}');".format(wh_name, frequency, app_db, source_table, model_name, target_table, append_mode, fun_sch, app_sch)
      sp_session.sql(task_stmt).collect()
      sp_session.sql("ALTER TASK IF EXISTS ENERGY_SPIKE_FORECASTING_INFERENCE_TASK RESUME").collect()
      return 'Task Created'



########################################################################################################################################################
#################################   SPIKE FORECASTING FUNCTIONALITY ####################################################################################
########################################################################################################################################################
@st.cache_data(ttl=900)
def perform_hyper_parameter_tuning_for_spike(_sp_session, app_db, table_name, model_name, n_trials, app_sch, fun_sch):
    """ Perform Hyper Parameter Tuning and return the results in a dataframe
    """   
    sql_call_stmt = "call {4}.{3}.sproc_optuna_optimized_model_spike('{0}','{1}',{2}, '{4}', '{5}')".format(table_name, model_name, n_trials, fun_sch, app_db, app_sch)
    out_str = _sp_session.sql(sql_call_stmt).collect() 
    #st.write(out_str)
    hyper_df = pd.DataFrame()  
    if out_str[0][0] == 'Success':
        hyper_df = _sp_session.sql('select * from {0}.{1}.HYPER_PARAMETER_OUTPUT_SPIKE;'.format(app_db, app_sch)).to_pandas()
        return hyper_df

@st.cache_data(ttl=900)
def perform_inferencing(_sp_session, app_db, table_name, app_sch, fun_sch):
    """ Perform Hyper Parameter Tuning and return the results in a dataframe
    """  
    sql_call_stmt = "call {2}.{1}.sproc_spike_forecast('{0}', '{2}', '{3}')".format(table_name, fun_sch, app_db, app_sch)
    out_str = _sp_session.sql(sql_call_stmt).collect() 
    #st.write(out_str)
    hyper_df = pd.DataFrame()  
    if out_str[0][0] == 'Success':
        hyper_df = _sp_session.sql('select * from {0}.{1}.FORECASTED_RESULT;'.format(app_db, app_sch)).to_pandas()
        return hyper_df

#@st.cache_data(ttl=900)
def get_database_list(_sp_session, import_flag, app_flag):
    """ Get the database list for auto-displaying. 
    """  
    if import_flag:
      get_command = "select database_name  \
                    from information_schema.databases where type = 'IMPORTED DATABASE' \
                        order by created desc, database_name;"   
      
    elif app_flag:
        get_command = "select database_name  \
                    from information_schema.databases where type = 'APPLICATION' \
                        order by created desc, database_name;"   
        
    else:
      get_command = "select database_name  \
                        from information_schema.databases where type = 'STANDARD' \
                              order by created desc, database_name;"   
    
    df = _sp_session.sql(get_command).to_pandas()
    return df['DATABASE_NAME'].values.tolist()

@st.cache_data(ttl=100)
def get_table_views_list_from_db(_sp_session, database):
    """ Get the views list for auto-displaying. 
    """  
    #_sp_session.use_database('{0}'.format(database))

    get_views_sql_stmt = "select CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) as VIEWS from information_schema.tables  \
                            where table_schema != 'INFORMATION_SCHEMA' order by table_schema, table_name;"
    df = _sp_session.sql(get_views_sql_stmt).to_pandas() 
    return df['VIEWS'].values.tolist()






# Get the current credentials -- App way
# UNCOMMENT FOR NATIVE APP STARTS ##
sp_session = get_active_session()
# UNCOMMENT FOR NATIVE APP ENDS ##

# COMMENT FOR NATIVE APP STARTS ##
# Get the current credentials -- Local Streamlit way
# import sflk_base as L

# # Define the project home directory, this is used for locating the config.ini file
# PROJECT_HOME_DIR='.'
# def initialize_snowpark():
#     if "snowpark_session" not in st.session_state:
#         config = L.get_config(PROJECT_HOME_DIR)
#         sp_session = L.connect_to_snowflake(PROJECT_HOME_DIR)
#         sp_session.use_role(f'''{config['APP_DB']['role']}''')
#         sp_session.use_schema(f'''{config['APP_DB']['database']}.{config['APP_DB']['schema']}''')
#         sp_session.use_warehouse(f'''{config['APP_DB']['warehouse']}''')
#         st.session_state['snowpark_session'] = sp_session

#     else:
#         config = L.get_config(PROJECT_HOME_DIR)
#         sp_session = st.session_state['snowpark_session']
    
#     return (config, sp_session)

# COMMENT FOR NATIVE APP ENDS ##

# App
st.title("Energy Price Forecasting Native App")
st.write(
   """This Snowflake Native App allows users to forecast wholesale energy prices and the likelihood of price spikes using data from Yes Energy. The structure below is broken into two pieces: forecasting energy prices as well as forecasting the likelihood of energy price spikes. 
   """
)

with st.container():
   # UNCOMMENT FOR NATIVE APP STARTS ##
   app_db = sp_session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
   app_sch = 'CRM'
   fun_db = app_db
   fun_sch = 'PYTHON_FUNCTIONS'
   app_flag = True
   # UNCOMMENT FOR NATIVE APP ENDS ##

   # COMMENT FOR NATIVE APP STARTS ##
   # config, sp_session = initialize_snowpark()
   # app_db = config['APP_DB']['database']
   # app_sch = config['APP_DB']['schema']
   # fun_db = app_db
   # fun_sch = app_sch
   # app_flag = False
   # COMMENT FOR NATIVE APP ENDS ##
   colxx, colxxx, colyy, colx, coly, colz = st.columns(6)
   with colz:
      if st.button('Refresh Page', use_container_width=False, key='refresh1'):
         st.experimental_rerun()
   st.header("Forecasting Energy Prices")

   
   hyper_parameter_output = 'hyper_parameter_output'
   stage_name = 'ML_MODELS'
   data_array, cols_list, features_list=[],[],[]
   target_selected=''
   if app_flag:
      table_to_select = 'ML_DATA_VW'
   else:
      table_to_select = 'ML_DATA'

   st.subheader(':blue[Feature Selection]')
   with st.expander("Feature Selection"):
      st.write("In this section, you will select the tables and features for the model training and testing. For the sake of simplicity, this demo does not show any feature engineering.")
      db_list = get_database_list(sp_session, False, app_flag)
      
      col1, col2, col33 = st.columns(3)
      with col1:
          database = st.selectbox(
                            'Select the database with features and target',
                            db_list)
          
      with col2:
          if database:
            list_of_tables = get_table_views_list_from_db(sp_session, database)
            try:
               index = list_of_tables.index("{0}.{1}".format(app_sch, table_to_select))
            except Exception as e:
                index = 0
            table_selected = st.selectbox(
               'Select the source table/view with features and target',
               list_of_tables)
            
      with col33:
          cols_list = get_column_list(sp_session, table_selected)
          if database and table_selected:
              cols_list = get_column_list(sp_session, table_selected)
              if len(cols_list) > 0:
               target_selected = st.selectbox(
                  'Select the target column',
                  cols_list, index=len(cols_list)-2)
              

      coli, colj, colk = st.columns(3)
      with coli:
         if database and table_selected: 
            try:
               min_date = sp_session.sql("select max(datetime)::date AS MAXDATE , min(datetime)::date as MINDATE from {0}.{1};".format(database, table_selected)).collect()[0][1]
               start_date = st.date_input(
               "Pick the start date",
               min_date, min_value=datetime.date(1996, 10,1),key='3')
            except Exception as e:
               min_date = date.today()
               start_date = st.date_input(
               "Pick the start date",
               min_date, min_value=datetime.date(1996, 10,1),key='33')
      with colj:
         if database and table_selected: 
            try:
               max_date = sp_session.sql("select max(datetime)::date AS MAXDATE , min(datetime)::date as MINDATE from {0}.{1};".format(database, table_selected)).collect()[0][0]
               end_date = st.date_input(
               "Pick the end date",
               max_date, max_value=date.today(), key='4')
            except Exception as e:
               max_date = date.today()
               end_date = st.date_input(
               "Pick the end date",
               max_date, max_value=date.today(), key='4')
      with colk:
          if len(cols_list) > 0:
            cols_list_features = cols_list.copy()
            try:
                cols_list_features.remove('RTPRICE_TOMM')
                cols_list_features.remove('DATETIME')
            except Exception as e:
                print(e)
            features_list = st.multiselect(
                  'Select the features to use in the model',
                  cols_list_features
              )
      if st.button('SELECT FEATURES'):
         view_exists_flag = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'ML_DATA_VW')).collect()[0][0]
         if view_exists_flag and app_flag:
            sp_session.sql("CREATE OR REPLACE VIEW {1}.{2}.ML_DATA_FACTOR_VW as SELECT {0},{6}, DATETIME FROM {1}.{3} WHERE \
                              datetime >= '{4} 00:00:00.000'::datetime \
                              and datetime <= '{5} 00:00:00.000'::datetime;".format(('%s' % ', '.join(map(str, features_list))), app_db, app_sch,  table_selected, start_date, end_date, target_selected )).collect()
         
            sp_session.sql("CREATE OR REPLACE VIEW {1}.{2}.ML_DATA_FACTOR_SPIKE_VW as SELECT {0},{6}, DATETIME FROM {1}.{2}.ML_DATA_SPIKE_VW WHERE \
                              datetime >= '{4} 00:00:00.000'::datetime \
                              and datetime <= '{5} 00:00:00.000'::datetime;".format(('%s' % ', '.join(map(str, features_list))), app_db, app_sch,  table_selected, start_date, end_date, target_selected )).collect()
         else:
             sp_session.sql("CREATE OR REPLACE VIEW {1}.{2}.ML_DATA_VW as SELECT {0},{6}, DATETIME FROM {1}.{3} WHERE \
                              datetime >= '{4} 00:00:00.000'::datetime \
                              and datetime <= '{5} 00:00:00.000'::datetime;".format(('%s' % ', '.join(map(str, features_list))), app_db, app_sch,  table_selected, start_date, end_date, target_selected )).collect()
         
               
             sp_session.sql("CREATE OR REPLACE VIEW {1}.{2}.ML_DATA_SPIKE_VW as SELECT {0},{6}, DATETIME FROM {1}.{3}_SPIKE WHERE \
                              datetime >= '{4} 00:00:00.000'::datetime \
                              and datetime <= '{5} 00:00:00.000'::datetime;".format(('%s' % ', '.join(map(str, features_list))), app_db, app_sch,  table_selected, start_date, end_date, target_selected )).collect()
         st.success('features selected successfully')
   #st.info(':orange[NOTE: The implementation of the selected features are not included into the demo yet, by default all the features from the table is used and target variable of PRICE is chosen currently]')
   st.subheader(':blue[Exploration]')
   with st.expander("Exploration"):
         st.write('In this section, we are plotting the different features and target.')
         #sp_session.use_database('{0}'.format(app_db))
         if app_flag:
            table_name = 'ML_DATA_FACTOR_VW'
         else:
            table_name = 'ML_DATA_VW'
         try:
            df = sp_session.sql("SELECT * FROM {2}.{1}.{0};".format(table_name, app_sch, app_db)).to_pandas()
         except Exception as e:
             df = pd.DataFrame()
         if df.empty:
             st.info("Please select features before proceeding")
         else:
            df = df.set_index('DATETIME')
            st.subheader('Exploration')
            col2, col3 = st.columns(2)
            with col2:
               st.dataframe(df.head(20))
            with col3:
               st.line_chart(data=df, y='RTPRICE_TOMM', use_container_width=True)
            
            st.subheader('Plotting Correlation')
            col2, col3 = st.columns(2)
            
            with col2:
               correlation_df=df.copy()
               cor_data = (correlation_df
                              .corr().stack()
                              .reset_index()     # The stacking results in an index on the correlation values, we need the index as normal columns for Altair
                              .rename(columns={0: 'correlation', 'level_0': 'Features', 'level_1': 'Features2'}))
               cor_data['correlation_label'] = cor_data['correlation'].map('{:.2f}'.format)  # Round to 2 decimal
               #st.dataframe(cor_data)
               base = at.Chart(cor_data).encode(
                  x='Features2:O',
                  y='Features:O'    
               )

               # Text layer with correlation labels
               # Colors are for easier readability
               text = base.mark_text().encode(
                  text='correlation_label',
                  color=at.condition(
                     at.datum.correlation > 0.5, 
                     at.value('white'),
                     at.value('black')
                  )
               )
               # The correlation heatmap itself
               cor_plot = base.mark_rect().encode(
                  color='correlation:Q'
               )
               cor_plot + text  
                        
   st.subheader(':blue[Hyperparameter Optimization]')
   with st.expander("Hyperparameter Optimization"):
      st.subheader('Hyperparameter Optimization')
      model_name = 'optuna_model.sav'
      n_trials = 500
      hyper_df = pd.DataFrame()
      #sp_session.use_database('{0}'.format(app_db))
      hyper_tuning_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'HYPER_PARAMETER_OUTPUT')).collect()[0][0]
      st.write("We use two open source packages, Optuna and CMAES, to optimize the hyperparameters of the model. You can see the selected hyperparameters below. ")
      if hyper_tuning_done:
            hyper_df = sp_session.sql('SELECT * FROM {2}.{1}.{0};'.format('HYPER_PARAMETER_OUTPUT', app_sch, app_db)).to_pandas()
            if st.button("Run hyperparameter optimization", key='111'):
               hyper_df = perform_hyper_parameter_tuning(sp_session, app_db, table_name, model_name, n_trials, app_sch, fun_sch)
      else:
            
            if st.button("Run hyperparameter optimization", key='Z1'):
               hyper_df = perform_hyper_parameter_tuning(sp_session, app_db, table_name, model_name, n_trials, app_sch, fun_sch)
      st.dataframe(hyper_df.head(20), use_container_width=True)
   
   st.subheader(':blue[Backtesting and Evaluation]')
   with st.expander("Backtesting and Evaluation"):
         st.write("In this section, you will run a backtest on a 1-year test period, using walk-forward cross-validation. In the backtest, the model will forecast prices for the next day with only data available up to that point (i.e. no data leakage). The image below allows you to show forecasts versus actuals by day. ")
         df['DATETIME']=[str(i) for i in df.index]
         start_threshhold=df[['2020' in i or '2021' in i or '2022' in i for i in df.DATETIME]].shape[0]
         splits=[i for i in range(start_threshhold-7, df.shape[0], 7*24) if i+7*24< df.shape[0]]
         scores=[]
         col5, col6 = st.columns(2) 
         #st.write(scores)
         with col5:
            if st.button("Run Backtest"):
               for i in tqdm(splits):
                     split = i
                     sql_call_stmt = "call {3}.{2}.sproc_final_model('{0}',{1}, '{3}','{4}')".format(table_name, split, fun_sch, app_db, app_sch)
                     best_trial_params = sp_session.sql(sql_call_stmt).collect()
                     scores.append(best_trial_params)
            #sp_session.use_database('{0}'.format(app_db))
            prediction_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'PREDICTIONS')).collect()[0][0]
            if prediction_done:
               distinct_dates = get_distinct_dates_predicted(sp_session, app_db, app_sch)
               date_selected = st.selectbox('Select the day for which to view the actuals versus predictions in the backtest.', distinct_dates)
               df_pred=sp_session.sql("SELECT * FROM {1}.{2}.PREDICTIONS where concat(DATE_PART('YEAR',to_timestamp(\"Datetime\")), \
                                       '-', LPAD(DATE_PART('MM',to_timestamp(\"Datetime\")),2,0),'-', LPAD(DATE_PART('DD', \
                                       to_timestamp(\"Datetime\")),2,0)) = '{0}'".format(date_selected, app_db, app_sch)).to_pandas()
               df_pred = df_pred.set_index('Datetime')
               st.line_chart(data=df_pred, y=['Ground_Truth','Predictions'], use_container_width=True)
               
   with st.expander("View Backtest Results and Performance"):
      try:
         if prediction_done:
            #Call predictions table and get results from March 2021 to end of 2021
            results = get_predictions_results(sp_session, app_db, app_sch, 'PREDICTIONS')
            results1=results[["2020" not in i and "2021-01" not in i and "2021-02" not in i for i in results.Datetime]]
            results['Persistence24HOUR']=results.Ground_Truth.shift(24)
            results['Persistence48HOUR']=results.Ground_Truth.shift(48)
            #Define results and metrics of model
            #Get final TLDR metric performances comparing Myst models with Snowflake models
            preds=np.array(results1.Predictions)
            truth=np.array(results1.Ground_Truth)

            mae=sum(np.abs(np.subtract(preds,truth)))/len(preds)
            mse=sum(np.subtract(preds,truth)**2)/len(preds)

            mae24=sum(np.abs(np.subtract(results['Persistence24HOUR'][24:],truth[24:])))/len(results['Persistence24HOUR'][24:])
            mse24=sum(np.subtract(results['Persistence24HOUR'][24:],truth[24:])**2)/len(results['Persistence24HOUR'][24:])

            mae48=sum(np.abs(np.subtract(results['Persistence48HOUR'][48:],truth[48:])))/len(results['Persistence48HOUR'][48:])
            mse48=sum(np.subtract(results['Persistence48HOUR'][48:],truth[48:])**2)/len(results['Persistence48HOUR'][48:])

            #add prior results
            results_df=pd.DataFrame({'MSE': [mse24, mse48, mse],'MAE': [mae24,mae48, mae]})
            results_df.index=['Lambda - 24H Persistence',
                           'Lambda - 48H Persistence','Snowflake Price Forecast']
            st.dataframe(results_df)
            st.line_chart(results1, x='Datetime', y=['Predictions', 'Ground_Truth'])
            st.line_chart(results1, x='Datetime', y='MAE')
            st.line_chart(results1, x='Datetime', y='MSE')

      except Exception as e:
          st.info("Perform Forecasting to view the results")

with st.container():
   st.header("Model Deployment")
   coli, colj, colk = st.columns(3)
   with coli:
         tables = get_tables_list(sp_session, app_db)
         if len(tables) > 0:
            source_table_name = st.selectbox(
                  'Please select the source table',
                  tables, key='price1')
         else:
            source_table_name = st.text_input(
                           "Please select the source table","ENGY_FORECASTING_SOURCE",
                           key="sourcetable",
                        )
   with colj:
         target_table_name = st.text_input(
                           "Please provide a name for the target table","ENGY_FORECASTING_TARGET",
                           key="targettable",
                        )
   with colk:
         frequency = st.selectbox(
               'Please select how often to generate a new forecast (in minutes)',
               range(15, 300, 15), key='price2')
   coll, colm, coln = st.columns(3)
   with coll:
         models = get_models_list(sp_session, app_db, stage_name, app_sch)
         if len(models) > 0:
            Model_Name = st.selectbox(
                  'Please select the model',
                  models, key='price3')   
         else:
            Model_Name = 'forecast_2020-12-31 16:00:00.sav'
            st.info('Please run the forecasting to create Model files in stage')
   with colm:
         warehouses = get_warehouses_list(sp_session)
         if len(warehouses) > 0:
            warehouse_name = st.selectbox('Please select the warehouse ',
               warehouses, key='price4')
         else:
            warehouse_name = st.text_input(
                           "Please select the warehouse","COMPUTE_WH",
                           key="warehouse",
                        )
             
   with coln:
      append_mode = st.selectbox(
         'Run inference once or Schedule the inference',
         ('True', 'False'))

if st.button('Deploy Model', key='price6', use_container_width=True):
   out_str = deploy_price_model(sp_session, app_db, Model_Name, frequency, source_table_name, target_table_name, warehouse_name, append_mode, app_sch, fun_sch)
   if out_str == 'Dynamic Table Created':
      st.success('Dynamic Table created with the parameters chosen!', icon="✅")
      df_pred = sp_session.sql("SELECT * FROM {0}.{1}.{2} order by DATETIME DESC limit 30".format(app_db, app_sch, target_table_name)).to_pandas()
            
   elif out_str == 'Initial Load':
      st.success('Forecasting Performed for the Source Table : {0}'.format(source_table_name), icon="✅")
      df_pred = sp_session.sql("SELECT * FROM {0}.{1}.{2} order by DATETIME DESC limit 30".format(app_db, app_sch, target_table_name)).to_pandas()
   st.dataframe(df_pred)

st.subheader(':blue[View Predicted Results]')
pred_sel_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name in ('{1}')".format(app_sch, target_table_name)).collect()[0][0]
if pred_sel_done:
   st.dataframe(sp_session.sql("SELECT * FROM {0}.{1}.{2} order by DATETIME DESC limit 30".format(app_db, app_sch, target_table_name)).to_pandas())



########################################################################################################################################################
#################################   SPIKE FORECASTING FUNCTIONALITY ####################################################################################
########################################################################################################################################################
with st.container():
   if app_flag:
      factor_sel_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name in ('{1}')".format(app_sch, 'ML_DATA_FACTOR_SPIKE_VW')).collect()[0][0]
   else:
      factor_sel_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name in ('{1}')".format(app_sch, 'ML_DATA_SPIKE_VW')).collect()[0][0]
   if factor_sel_done and app_flag:
      table_name_spike = 'ML_DATA_FACTOR_SPIKE_VW'
   elif not factor_sel_done and app_flag:
      table_name_spike = 'ML_DATA_SPIKE_VW'
   elif factor_sel_done and not app_flag:
      table_name_spike = 'ML_DATA_SPIKE_VW'
   else:
      table_name_spike = 'ML_DATA_SPIKE'
   stage_name = 'ML_SPIKE_MODELS'
   data_array = []
   st.title("Perform Energy Spike Forecasting")
   spike_table_present = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, table_name_spike)).collect()[0][0]
   if spike_table_present:
      df_spike = sp_session.sql("SELECT * FROM {2}.{1}.{0};".format(table_name_spike, app_sch, app_db)).to_pandas()
      df_spike = df_spike.set_index('DATETIME')
      #app_wh = sp_session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]
      st.subheader(':blue[Exploration]')
      with st.expander("Exploration"):
         st.subheader('Exploration')
         st.write("In this section, we are plotting the different features and target.")
         col2, col3 = st.columns(2)
         with col2:
            st.dataframe(df_spike.head(20))
         with col3:
            st.line_chart(data=df_spike, y='RTPRICE_TOMM', use_container_width=True)

         st.subheader('Plotting Correlation')
         col222, col333 = st.columns(2)
         
         with col222:
            correlation_df=df_spike.copy()
            cor_data = (correlation_df
                              .corr().stack()
                              .reset_index()     # The stacking results in an index on the correlation values, we need the index as normal columns for Altair
                              .rename(columns={0: 'correlation', 'level_0': 'Features', 'level_1': 'Features2'}))
            cor_data['correlation_label'] = cor_data['correlation'].map('{:.2f}'.format)  # Round to 2 decimal
            #st.dataframe(cor_data)
            base = at.Chart(cor_data).encode(
               x='Features2:O',
               y='Features:O'    
            )

            # Text layer with correlation labels
            # Colors are for easier readability
            text = base.mark_text().encode(
               text='correlation_label',
               color=at.condition(
                  at.datum.correlation > 0.5, 
                  at.value('white'),
                  at.value('black')
               )
            )
            # The correlation heatmap itself
            cor_plot = base.mark_rect().encode(
               color='correlation:Q'
            )
            cor_plot + text    
   else:
      st.info("Perform the Fetch from Yes Energy to create views to your local SF account")
            
   st.subheader(':blue[Hyperparameter Optimization]')
   with st.expander("Hyperparameter Optimization"):
      model_name = 'optuna_model_s.sav'
      n_trials = 500
      st.subheader('Hyperparameter Optimization')
      st.write("We use two open source packages, Optuna and CMAES, to optimize the hyperparameters of the model. You can see the selected hyperparameters below. ")
      hyper_df = pd.DataFrame()
      hyper_tuning_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'HYPER_PARAMETER_OUTPUT_SPIKE')).collect()[0][0]
      if hyper_tuning_done:
            hyper_df = sp_session.sql('SELECT * FROM {1}.{0};'.format('HYPER_PARAMETER_OUTPUT_SPIKE', app_sch)).to_pandas()
      else:
            if st.button("Run hyperparameter optimization", key='22'):
               hyper_df = perform_hyper_parameter_tuning_for_spike(sp_session, app_db, table_name_spike, model_name, n_trials, app_sch, fun_sch)
      st.dataframe(hyper_df.head(20), use_container_width=True)

   st.subheader(':blue[Perform threshold analysis]')
   with st.expander("Perform threshold analysis"):
      col6, col7, col8 = st.columns(3)
      with col6:
         st.subheader('Precision Recall Curve')
         hyper_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'FORECASTED_RESULT')).collect()[0][0]
         if hyper_done:                   
            scored_sdf = sp_session.sql("SELECT * FROM {0}.{2}.{1}".format(app_db, 'FORECASTED_RESULT', app_sch)).to_pandas()
            scored_sdf = scored_sdf[['PREDICTION', 'RTPRICE_TOMM']]
            y_pred = scored_sdf['PREDICTION']
            y_true = scored_sdf['RTPRICE_TOMM']
            fpr, tpr, t = roc_curve(y_true, y_pred)
            auc = roc_auc_score(y_true, y_pred)
            p, r, c = precision_recall_curve(y_true, y_pred)
            ave_precision = average_precision_score(y_true, y_pred)
            f1 = 2*p*r/(p+r)
            t = c[f1.argmax()]
            fig, axis = plt.subplots()
            plt.plot(r, p)
            plt.xlabel('recall')
            plt.ylabel('precision')
            plt.title(f'Precision Recall Curve\nAP Distance Numeric={ave_precision:.3f} ')
            st.plotly_chart(fig, use_container_width=True)
         else:
            if st.button('Run Threshold Analysis'):
               scored_sdf = perform_inferencing(sp_session, app_db, table_name_spike, app_sch, fun_sch)
               scored_sdf = scored_sdf[['PREDICTION', 'RTPRICE_TOMM']]
               y_pred = scored_sdf['PREDICTION']
               y_true = scored_sdf['RTPRICE_TOMM']
               fpr, tpr, t = roc_curve(y_true, y_pred)
               auc = roc_auc_score(y_true, y_pred)
               p, r, c = precision_recall_curve(y_true, y_pred)
               ave_precision = average_precision_score(y_true, y_pred)
               f1 = 2*p*r/(p+r)
               t = c[f1.argmax()]
               fig, axis = plt.subplots()
               plt.plot(r, p)
               plt.xlabel('recall')
               plt.ylabel('precision')
               plt.title(f'Precision Recall Curve\nAP Distance Numeric={ave_precision:.3f} ')
               st.plotly_chart(fig, use_container_width=True)
         
      with col7:
         st.subheader('Plot ROC Curve')
         hyper_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'FORECASTED_RESULT')).collect()[0][0]
         if hyper_done:
            fig, axis = plt.subplots()
            plt.plot(fpr, tpr)
            plt.xlabel('fpr')
            plt.ylabel('tpr')
            plt.title(f'ROC Curve\nROC AUC={auc:.3f}')
            st.plotly_chart(fig, use_container_width=True)
               
      with col8:
         st.subheader('Threshhold with maximum F1 Score')
         hyper_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'FORECASTED_RESULT')).collect()[0][0]
         if hyper_done:
            #st.write("#### Threshhold that maximizes F1 Score:")
            pcr=pd.DataFrame({'Threshhold':c, 'Precision':p[:-1],'Recall':r[:-1], 'AUC':auc})
            pcr[pcr.Threshhold==t]

   st.subheader(':blue[Backtesting and Evaluation]')
   with st.expander("Backtesting and Evaluation"):
      st.write("In this section, you will run a backtest on a 1-year test period, using walk-forward cross-validation. In the backtest, the model will forecast prices for the next day with only data available up to that point (i.e. no data leakage). The image below allows you to show forecasts versus actuals by day. ")
      if spike_table_present:
         df_spike['DATETIME']=[str(i) for i in df_spike.index]
         start_threshhold=df_spike[['2020' in i or '2021' in i or '2022' in i for i in df_spike.DATETIME]].shape[0]
         #start_threshhold=df_spike[['2017' in i or '2018' in i or '2019' in i or '2020' in i for i in df_spike.DATETIME]].shape[0]
         splits=[i for i in range(start_threshhold-7, df_spike.shape[0], 7*24) if i+7*24< df_spike.shape[0]]
         scores=[]
         col5, col6, col7, col8 = st.columns(4)
         with col5:
               if st.button("Run Backtest", key='333'):
                  for i in splits:
                     split = i
                     sql_call_stmt = "call {3}.{2}.sproc_final_model_spike('{0}',{1}, '{3}', '{4}')".format(table_name_spike, split, fun_sch, app_db, app_sch)
                     best_trial_params = sp_session.sql(sql_call_stmt).collect()
                     scores.append(best_trial_params)
                  st.info("Forecasting is completed")
               prediction_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name = '{1}'".format(app_sch, 'PREDICTIONS1')).collect()[0][0]
               #scored_sdf = test.with_column('PREDICTION', udf_score_optuna_model_vec_cached(*feature_cols))
   
   st.subheader(':blue[View Backtest Results and Performance]')
   with st.expander("View Backtest Results and Performance"):
      if prediction_done:
            results = get_predictions_results(sp_session, app_db, app_sch, 'PREDICTIONS1')
            results1=results[["2023" in i for i in results.Datetime]]
            results1['Persistence24HOUR']=results1.Ground_Truth.shift(24)
            results1['Persistence48HOUR']=results1.Ground_Truth.shift(48)
            #Define results and metrics of model
            preds=results1.Predictions.values
            truth=results1.Ground_Truth
            auc24 = roc_auc_score(truth[24:], results1['Persistence24HOUR'][24:].astype(bool))
            ap24 = average_precision_score(truth[24:], results1['Persistence24HOUR'][24:].astype(bool))
            r24=recall_score(truth[24:], results1['Persistence24HOUR'][24:].astype(bool))
            p24=precision_score(truth[24:], results1['Persistence24HOUR'][24:].astype(bool))

            auc48 = roc_auc_score(truth[48:], results1['Persistence48HOUR'][48:].astype(bool))
            ap48 = average_precision_score(truth[48:], results1['Persistence48HOUR'][48:].astype(bool))
            r48=recall_score(truth[48:], results1['Persistence48HOUR'][48:].astype(bool))
            p48=precision_score(truth[48:], results1['Persistence48HOUR'][48:].astype(bool))

            auc = roc_auc_score(truth, preds)
            ap = average_precision_score(truth, preds)
            r=recall_score(truth, preds)
            p=precision_score(truth, preds)
            #results_df=pd.DataFrame({'MSE': [mse],'MAE': [mae]})
            #results_df.index=['Snowflake Price Forecast']
            st.dataframe(results1)
            st.line_chart(results1, x='Datetime', y=['Predictions', 'Ground_Truth'])
            st.line_chart(results1, x='Datetime', y='AP')
            st.line_chart(results1, x='Datetime', y='Precision')
            st.line_chart(results1, x='Datetime', y='Recall')
            st.line_chart(results1, x='Datetime', y='AUC')
            resultso = get_predictions_results(sp_session, app_db, app_sch, 'PREDICTIONS')
            #narrow results to March 2021 to end of 2021
            resultso1=resultso[["2023"  in i for i in resultso.Datetime]]
            # resultso1=resultso[["2020" not in i and "2021-01" not in i and "2021-02" not in i and "2021-02" not in i for i in resultso.Datetime]]
            preds1=resultso1.Predictions.values>50
            truth1=resultso1.Ground_Truth>50
            nauc = roc_auc_score(truth1, preds1)
            nap = average_precision_score(truth1, preds1)
            n_r=recall_score(truth1, preds1)
            n_p=precision_score(truth1, preds1)

            results_df=pd.DataFrame({'Precision':[p24,p48,p,n_p], 'Recall': [r24,r48,r,n_r],'AUC': [auc24,auc48,auc,nauc]})
            results_df=pd.concat([results_df, pcr[pcr.Threshhold==t][['Precision', 'Recall', 'AUC']]])
            results_df.index=['Lambda - 24H Persistence','Lambda - 48H Persistence',
                           'Snowflake Spike Forecast', 'Snowflake Price Forecast','Snowflake One Time Train']
            results_df.style.format(precision=2).highlight_max(color="lightgreen", axis=0)
            st.subheader('Our Backtesting Results')
            st.dataframe(results_df)

with st.container():
   st.header("Spike Model Deployment")
   coli, colj, colk = st.columns(3)
   with coli:
         tables = get_tables_list(sp_session, app_db)
         if len(tables) > 0:
            source_table_name = st.selectbox(
                  'Please select the source table',
                  tables, key='pricespike1')
         else:
            source_table_name = st.text_input(
                           "Please select the source table","ENGY_SPIKE_FORECASTING_SOURCE",
                           key="sourcetable2",
                        )
   with colj:
         target_table_name = st.text_input(
                           "Please provide a name for the target table","ENGY_SPIKE_FORECASTING_TARGET",
                           key="targettable2",
                        )
   with colk:
         frequency = st.selectbox(
               'Please select how often to generate a new forecast (in minutes)',
               range(15, 300, 15), key='pricespike2')
   coll, colm, coln = st.columns(3)
   with coll:
         models = get_models_list(sp_session, app_db, stage_name, app_sch)
         if len(models) > 0:
            Model_Name = st.selectbox(
                  'Please select the model',
                  models, key='pricespike3')   
         else:
            Model_Name = 'forecast_2020-12-31 16:00:00.sav'
            st.info('Please run the forecasting to create Model files in stage')
   with colm:
         warehouses = get_warehouses_list(sp_session)
         if len(warehouses) > 0:
            warehouse_name = st.selectbox('Please select the warehouse',
               warehouses, key='pricespike4')
         else:
            warehouse_name = st.text_input(
                           "Please select the warehouse","COMPUTE_WH",
                           key="warehousespike",
                        )
             
   with coln:
      append_mode = st.selectbox(
         'Run inference once or Schedule the inference',
         ('True', 'False'), key='spike')

if st.button('Deploy Spike Model', key='pricespike6', use_container_width=True):
   out_str = deploy_spike_model(sp_session, app_db, Model_Name, frequency, source_table_name, target_table_name, warehouse_name, append_mode, fun_sch, app_sch)
   if out_str == 'Task Created':
      st.success('ENERGY_FORECASTING_INFERENCE_TASK --> Task Created', icon="✅")
      df_pred = sp_session.sql("SELECT * FROM {0}.{1}.PREDICTIONS1 limit 30".format(app_db, app_sch, target_table_name)).to_pandas()
      
   elif out_str == 'Initial Load':
      st.success('Forecasting Performed for the Source Table : {0}'.format(source_table_name), icon="✅")
      df_pred = sp_session.sql("SELECT * FROM {0}.{1}.PREDICTIONS1 limit 30".format(app_db, app_sch, target_table_name)).to_pandas()
      
st.subheader(':blue[View Predicted Results]')
pred_sel_done = sp_session.sql("select to_boolean(count(1)) from information_schema.tables where table_schema = '{0}' and table_name in ('{1}')".format(app_sch, 'PREDICTIONS1')).collect()[0][0]
if pred_sel_done:
   st.dataframe(sp_session.sql("SELECT * FROM {0}.{1}.{2} limit 30".format(app_db, app_sch, 'PREDICTIONS1')).to_pandas())

