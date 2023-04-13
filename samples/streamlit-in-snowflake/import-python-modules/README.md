# Import Python files from custom module/folder

If you have a folder full of python files that you want to import into your app, you can create a zip folder, put the zipped folder in the stage, and then import from that zip directly.

## Create the zip module
Put python files in the `utils` folder and Zip the utils folder

   ```sh
   zip -r utils.zip utils/*.py
   ```

## Create app

### Using snowcli
Create the app 

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

Upload utils.zip to the stage

   ```sh
   snow stage put utils.zip GREAT_NAME_FOR_MY_STREAMLIT_STAGE
   ```

### Using snowsql
Create the stage and upload the files to it

   ```sql
   CREATE STAGE my_stage;
   PUT file://path-to-code-directory/streamlit_app.py @my_stage auto_compress=false overwrite=true;

   PUT file://path-to-code-directory/utils.zip @my_stage auto_compress=false overwrite=true;
   ```

Create the app

   ```sql
   CREATE STREAMLIT my_test_app
   ROOT_LOCATION = @<db>.<schema>.my_stage
   MAIN_FILE='/streamlit_app.py'
   QUERY_WAREHOUSE=<warehouse_name>

   ```