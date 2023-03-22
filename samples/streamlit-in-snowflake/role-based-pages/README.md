# Role-based page access

If you want to limit certain apps to only be visible to viewers who have access to
a specific role, that is possible today with the set_pages trick (that was formerly
necessary for MultiPage apps).

1. Create an app with the code from streamlit_app.py
2. Update the page list at the bottom of the streamlit_app.py file
   - Decide what roles users should have access to in order to see each page, if any
   ```python
    roles = get_current_roles()
    if "PUBLIC" in roles:
        pages.append(Page("public.py", "Public", "🌍"))
    ...
   ```
3. Test locally to make sure you can see all the pages in the sidebar that you expect to be able to see with the role that you are using

   ```sh
   streamlit run streamlit_app.py
   ```

4. Create the app in SiS, either through the web UI, or through the snowcli

   ```sh
   snow streamlit create --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   snow streamlit deploy --file streamlit_app.py GREAT_NAME_FOR_MY_STREAMLIT
   ```

5. Upload all of the other pages to the stage associated with your app

   ```sh
   snow stage put public.py GREAT_NAME_FOR_MY_STREAMLIT_STAGE
   # Other pages as well ...
   ```

6. Profit!
