import sys
import pandas as pd
from joblib import load
import sklearn
from _snowflake import vectorized

# vectorized decorator allows the udf to bulk process rows during inference to speed up processing
@vectorized(input=pd.DataFrame)
def evaluate_model(X: pd.DataFrame) -> pd.Series:

    # import model from package stage
    import_dir = sys._xoptions.get("snowflake_import_directory")
    model_file = import_dir + 'linreg_location_sales_model.sav'
    model = load(model_file)

    # run inference
    sales = model.predict(X)
    return sales
