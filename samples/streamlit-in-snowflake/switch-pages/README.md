# Switching Pages Programatically

If you are already using multiple pages in your app, you might want to automatically
trigger a page change for the app user when they take certain actions (e.g. when they
click a button, or select a certain value of a widget for a multi-page form).

1. Create an app with the code from streamlit_app.py
2. Add extra pages
3. Whenever you want to switch to a new page, just run

   ```python
   switch_page("page name")
   ```

4. Test locally to make sure you can see all the pages in the sidebar and that the pages
   switch when you expect them to.

   ```sh
   streamlit run streamlit_app.py
   ```

5. Create the app in SiS, either through the web UI, or through the snowcli

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

6. Upload all of the other pages to the stage associated with your app

   ```sh
   snow stage put pages/page_1.py GREAT_NAME_FOR_MY_STREAMLIT_STAGE/pages
   snow stage put pages/page_2.py GREAT_NAME_FOR_MY_STREAMLIT_STAGE/pages
   ```

7. Profit!
