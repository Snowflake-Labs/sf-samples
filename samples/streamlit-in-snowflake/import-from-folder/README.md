# Import from folder

If you have a folder full of modules that you want to import from your app, than
the trick from [Multiple python files](https://github.com/blackary/sis-tricks/tree/main/multiple-python-files)
won't work -- you can't just `put` a whole folder to the stage and then import from it.

Instead, you can create a zip folder, put _that_ to the stage, and then import from
that zip directly.

1. Create an app with the code from streamlit_app.py. Note the important step of adding `utils.zip` to `sys.path`
2. Put several python modules in the `utils` folder, or another similar
3. Zip the utils folder

   ```sh
   zip -r utils.zip utils/*.py
   ```

4. Create the app in SiS, either through the web UI, or through the snowcli

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

5. Upload utils.zip to the stage

   ```sh
   snow stage put utils.zip GREAT_NAME_FOR_MY_STREAMLIT_STAGE
   ```

6. Profit!
