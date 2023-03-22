# Import Python files from custom module/folder

If you have a folder full of python files that you want to import into your app, you can create a zip folder, put the zipped folder in the stage, and then import from that zip directly.

## Create the zip module
Put python files in the `utils` folder and Zip the utils folder

   ```sh
   zip -r utils.zip utils/*.py
   ```

## Create app
Create the app in SiS, either through the web UI, or through the snowcli

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

Upload utils.zip to the stage

   ```sh
   snow stage put utils.zip GREAT_NAME_FOR_MY_STREAMLIT_STAGE
   ```

