# Multi-page apps

UPDATE: Multipage apps are supported natively if you upload pages to the `pages/` folder
in the stage associated with your app.

1. ~~Create an app with the code from streamlit_app.py~~
2. ~~Update the page list at the bottom of the streamlit_app.py file~~
3. Test locally to make sure you can see all the pages in the sidebar

   ```sh
   streamlit run streamlit_app.py
   ```

4. Create the app in SiS, either through the web UI, or through the snowcli

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

5. Upload all of the other pages to the `pages/` folder in the stage associated with your app

   ```sh
   snow stage put secondary_page.py GREAT_NAME_FOR_MY_STREAMLIT_STAGE/pages
   ```

6. Profit!
