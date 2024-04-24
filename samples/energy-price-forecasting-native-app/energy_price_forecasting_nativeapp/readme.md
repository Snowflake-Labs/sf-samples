- # Energy Spike Forecasting
    ## Introduction
    This demo shows how to set up a recurring forecast for day-ahead hourly electricity price at a trading point in Texas (ERCOT North Hub). 

    Most electricity is sold in wholesale electricity markets. Power plant operators sell power from their generators while utilities or retail energy providers buy power to cover their load obligations. Often, these companies have dedicated trading, supply, or portfolio management teams responsible for the buying and selling of electricity. 

    Since the price of electricity can be highly volatile — fluctuating as much as 1,000x between different times in a single day — buying or selling electricity can drive very large gains or losses. To minimize risk and maximize profits, companies need accurate short-term forecasts of volumes (either supply or demand) as well as market prices. These short-term forecasts are then used by traders or portfolio managers to inform buying and selling decisions. 

    While this demo focuses explicitly on electricity price forecasting, it can be modified to focus on forecasting price for other commodities (e.g. oil and natural gas). 

- ### What are the steps in the app?
    The users of the app has to through the following steps.

- #### Model development
    - **Data collection**. Data is collected for the target (historical day-ahead prices for ERCOT North) and for relevant features (such as system-wide load forecasts and solar production forecasts). These data come via a free data sample from data provider Yes Energy, available through Snowflake Marketplace. This means that no ELT is needed. 
    - **Data processing**. Then, we clean the data so that it can be used in the model experimentation. This includes seeing if there are anomalies in the data and formatting the column titles. 
    - **Feature engineering**. Then, features must be constructed. We created a feature for Real Time Market (RTM) System Lambda prices from yesterday by doing a 24 hour shift on our Real Time Market (RTM) System Lambda prices from today. 
    - **Hyperparameter optimization**. We use two open source packages (Optuna and CMAES) to optimize the hyperparameters of the model. 
    - **Experimentation using backtesting**. We then backtest the model. We train the model on data from early 2017 to 2020 and make predictions on the first week of 2021. Then we retrain the model on the previous training data + the first week we made predictions on and predict upon the second week and continue retraining the model weekly to make our next batch of weekly predictions.
    - **Evaluating performance**. Finally, we visualize performance of the model, using both mean squared error (MSE) and mean absolute error (MAE). We find that the model performs significantly better than a persistence forecast. 

- #### Model deployment
    - **Scheduling model** inference or scoring. Finally, we included a method to produce a new forecast (i.e. run inference or scoring) from a trained model on a schedule. We do this by using a User Defined Function (UDF) to call the model from Snowflake’s ML staging area and perform inference on new data that enters Snowflake’s database. 

- #### Data Usage Examples:

    -  Since the data is also shared, you can very well access the data using the below commands. 
        ```sql
        SELECT * FROM <app_name>.CRM.ML_DATA;
        SELECT * FROM <app_name>.CRM.HYPER_PARAMETER_OUTPUT;
        ```
    -  This app can be used to view the yes energy data in the app and can be used in any existing pipelines too
    -  This all is the one way to perform price forecasting. There are various storeprocs deployed along with the app as below.
        ```sql
        CALL <app_name>.CRM.PYTHON_FUNCTIONS.SPROC_DEPLOY_MODEL(VARCHAR, VARCHAR, VARCHAR, VARCHAR) -- used for deployment
        CALL <app_name>.CRM.PYTHON_FUNCTIONS.SPROC_FINAL_MODEL(VARCHAR, NUMBER) -- used for forecasting

- ### Action Needed
    - Please run the below command in a worksheet to ensure the deployment is possible.
    ```sh
    GRANT USAGE ON WAREHOUSE {WARE_HOUSE_NAME}
        TO APPLICATION Energy_Price_Forecasting_App;
    ```
