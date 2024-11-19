# *************************************************************************************************************
# Script:             ShareWatch 1.0  
# Create Date:        2024-02-29
# Author:             Fady Heiba, Amit Gupta
# Description:        Streamlit app interfacing with ShareWatch objects and stored procedures that allow 
#                     Snowflake data share consumers to operationalize their shares into their mission-critical
#                     data pipelines.
# *************************************************************************************************************

# Add whitepaper link

# Import python packages
import streamlit as st
import pandas as pd
import json
import time
from datetime import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit

# Get the current credentials
session = get_active_session()

# Set page settings
st.set_page_config(page_title="ShareWatch", page_icon="⚙️", layout="centered")

# Capture current databases monitored
databases_monitored_df = session.table("SHAREWATCH.UTIL.DATABASES_MONITORED")

# Check if there are no databases being monitored
if databases_monitored_df.count() == 0:
    # Display the app title and introduction
    st.markdown("# ShareWatch")
    st.markdown(
        "Welcome! ShareWatch is part of a set of best practices to help data consumers operationalize their shares into their mission-critical data pipelines and mitigate the effects of schema drift. You can read **the full whitepaper here**."
    )
    st.divider()
    st.info('You currently have no shares tracked for schema changes. To get started, please add one or more shares to start monitoring them on a cadence of your choice.')

    # Display section title
    st.markdown("## Initial Configuration")  

    # Add shares to monitor
    st.markdown("###### Add Share Monitors")
    all_databases_df = pd.DataFrame(session.sql("SHOW DATABASES").collect())
    all_shares_df = all_databases_df[all_databases_df.kind=="IMPORTED DATABASE"]    
    shares_to_add = st.multiselect(
            label="Select share(s) to add",
            options=all_shares_df["name"]
    )
    
    # Set email alert recipient
    st.markdown("###### Set Email Alert Receipient")
    email_config = st.text_input('Add an email address to receive alerts when drift occurs', key='email_config')

    # Set task schedule
    st.markdown("###### Set Task Schedule")
    scheduler_col1, scheduler_col2 = st.columns([2,2])
    frequency_hour = scheduler_col1.selectbox('Run Frequency', key='frequency_hour', options=('0h','1h','2h','3h','4h','5h','6h','7h','8h','9h','10h','11h','12h','13h','14h','15h','16h','17h','18h','19h','20h','21h','22h','23h'))
    frequency_minute = scheduler_col2.selectbox('', key='frequency_minute', index=5, options=('0m','1m','2m','3m','4m','5m','6m','7m','8m','9m','10m','11m','12m','13m','14m','15m','16m','17m','18m','19m','20m','21m','22m','23m','24m','25m','26m','27m','28m','29m','30m','31m','32m','33m','34m','35m','36m','37m','38m','39m','40m','41m','42m','43m','44m','45m','46m','47m','48m','49m','50m','51m','52m','53m','54m','55m','56m','57m','58m','59m'))
    frequency_hour_num = int(str(frequency_hour).replace('h',''))
    frequency_minute_num = int(str(frequency_minute).replace('m',''))
    
    add_monitors_button_col1, add_monitors_button_col2 = st.columns([15.1,3])
    add_monitors_button = add_monitors_button_col2.button("Add " + ("Monitor" if len(shares_to_add) == 1 else "Monitors"))

    # Once the 'Add Monitors' button is clicked, set up the monitors selected
    if add_monitors_button:
        if len(shares_to_add) < 1:
            st.error('Please select a share to add a monitor for.')
        elif frequency_hour_num == 0 and frequency_minute_num < 5:
            st.error('Please choose a run frequency with a minimum of 0h 5m.')
        elif len(email_config) == 0 or '@' not in email_config:
            st.error('Please add a valid email address that you would like the alert emails to go to...')
        else:
            with st.spinner("Setting up your monitors..."):
                # Iterate over each monitor selected and run the setup sproc on it
                share_counter = 0
                for share_to_add in shares_to_add:
                    session.sql("call sharewatch.util.setup_schemadrift_monitor('" + share_to_add + "')").collect()
                    share_counter = share_counter + 1
                st.success("✅ " + str(share_counter) + (" monitor " if share_counter == 1 else " monitors ") + "successfully added to track! Scheduling master task...")

                # Schedule master task to keep track of share monitors
                cron_hour_wildcard_structure = '*' if frequency_hour_num == 0 else '*/' + str(frequency_hour_num)
                cron_minute_wildcard_structure = '*' if frequency_minute_num == 0 else '*/' + str(frequency_minute_num)
                frequency_in_cron = cron_minute_wildcard_structure + ' ' + cron_hour_wildcard_structure + ' * * *'

                session.sql("call sharewatch.util.configure_master_task('" + str(frequency_in_cron) + " UTC')").collect()               
                st.success("✅ Task scheduled successfully! Setting alert email...")

                # Set alert email recipient 
                session.sql("call sharewatch.util.update_app_config(\'notification_email\',\'{\"email_address\": \"" + str(email_config) + "\"}\');").collect()
                st.success("Alert email address set to: " + email_config + "! Starting first run of the monitors you just set up...")
    
                # Start the first run of the monitors recently set up
                session.sql("call sharewatch.util.run_schemadrift_monitor()").collect()
                st.info("You're all set! You will be rerouted to the Monitoring tab in a few seconds...")
                time.sleep(10)
                st.experimental_rerun() # Eventually replace with st.rerun after Streamlit in Snowflake passes 1.27.0


else:
    # Define tabs for the dashboard
    monitoring_tab, configuration_tab = st.tabs(["Monitoring", "Configuration"])

    with monitoring_tab:
        # Show section title and introduction
        st.markdown("# ShareWatch")
        st.markdown(
            "ShareWatch is part of a set of best practices to help data consumers operationalize their shares into their mission-critical data pipelines and mitigate the effects of schema drift. You can read **the full whitepaper here**."
        )
        st.divider()

        # Create columns for layout
        col1, col2, col3 = st.columns([8,4.5,2.75])
        col1.markdown("## Monitoring")

        # Pull data about the last run time of monitors
        runlog_df = session.table("SHAREWATCH.UTIL.SP_RUNLOG")
        runs_on_all_dbs_df = runlog_df.filter(col("SP_ARGUMENTS").substring(0,17)=='All Monitored DBs')        
        last_run_at_string = '1970-01-01 12:00:00'
        last_run_at_timestamp = datetime.strptime(last_run_at_string, '%Y-%m-%d %H:%M:%S')
        for row in runs_on_all_dbs_df.collect(): 
            if(row["SP_EXEC_TIME"]>last_run_at_timestamp):
                last_run_at_timestamp = row["SP_EXEC_TIME"]
        col1.caption('Last Run at: ' + last_run_at_timestamp.strftime('%b %d, %Y %I:%M:%S %p')) 

        # Show button to manually run monitors
        run_monitors_button = col3.button("Run Monitors")
        if run_monitors_button:
            with st.spinner("Running monitors..."):
                session.sql("execute task sharewatch.util.master_task;").collect()
                session.sql("call sharewatch.util.run_schemadrift_monitor()").collect()
                st.success("✅ Ran all share monitors successfully, you are now seeing the latest alerts!")
                runlog_df = session.table("SHAREWATCH.UTIL.SP_RUNLOG")
                runs_on_all_dbs_df = runlog_df.filter(col("SP_ARGUMENTS").substring(0,17)=='All Monitored DBs')        
                last_run_at_string = '1970-01-01 12:00:00'
                last_run_at_timestamp = datetime.strptime(last_run_at_string, '%Y-%m-%d %H:%M:%S')
                for row in runs_on_all_dbs_df.collect(): 
                    if(row["SP_EXEC_TIME"]>last_run_at_timestamp):
                        last_run_at_timestamp = row["SP_EXEC_TIME"]
                col1.caption('Just ran again at: ' + last_run_at_timestamp.strftime('%b %d, %Y %I:%M:%S %p')) 

        # Initialize lists to store outage and warning messages
        outage_share_msgs = []
        warning_share_msgs = []

        # Pull data about outages and warnings from monitored databases
        databases_monitored_df = session.table("SHAREWATCH.UTIL.DATABASES_MONITORED")
        for db in databases_monitored_df.collect():
            runlog_df = session.table("SHAREWATCH.UTIL.SP_RUNLOG")
            db_runlog_df = runlog_df.filter(col("SP_ARGUMENTS") == db["DB_NAME"])

            # Find the latest monitor run time for each database
            latest_db_monitor_run_string = '1970-01-01 12:00:00'
            latest_db_monitor_run_timestamp = datetime.strptime(last_run_at_string, '%Y-%m-%d %H:%M:%S')
            for row in db_runlog_df.collect(): 
                if(row["SP_EXEC_TIME"]>latest_db_monitor_run_timestamp):
                    latest_db_monitor_run_timestamp = row["SP_EXEC_TIME"]
            latest_db_monitor_run_df = db_runlog_df.filter(col("SP_EXEC_TIME") == latest_db_monitor_run_timestamp)
            latest_db_monitor_run = latest_db_monitor_run_df.collect()
            latest_db_monitor_msg = str(latest_db_monitor_run[0]["SP_EXEC_MSG"])

            # Check if the latest message is an outage or a warning
            if latest_db_monitor_msg[0:6] == 'Outage':
                outage_share_msgs.append(latest_db_monitor_run[0])
            elif latest_db_monitor_msg[0:7] == 'Warning':
                warning_share_msgs.append(latest_db_monitor_run[0])
            
        # Show success message if no issues are detected
        if len(outage_share_msgs) == 0 and len(warning_share_msgs) == 0:
            st.success("✅ All shares are as expected. No schema drift detected.")

        # Show outages if any are detected
        if len(outage_share_msgs) > 0:
            st.markdown("")
            st.markdown("#### 🔥 Outages")
            st.caption("We found the following outages on the shares below. Once you adjust your pipelines to the new state of these shares, feel free to reset the monitor status in the All Shares section below.")

            # Show details of each outage
            for msg in outage_share_msgs:
                st.markdown("**Database** " + msg["SP_ARGUMENTS"])
                full_outage_message = msg["SP_EXEC_MSG"]
                full_outage_list = full_outage_message.splitlines()
                st.error(full_outage_list[0])
                full_outage_list.remove(full_outage_list[0])
                full_outage_list.remove(full_outage_list[0])
                object_outage_table_df = pd.DataFrame(columns=['Object Type','Object Name'])
                
                for object_row in full_outage_list:
                    object_row_list = object_row.split(",")
                    object_type = object_row_list[0].split(": ")[1]
                    object_name = object_row_list[1].split(": ")[1]
                    object_row_parsed = [object_type, object_name]
                    object_row_parsed_df = pd.DataFrame(columns=object_outage_table_df.columns, data=[object_row_parsed])
                    object_outage_table_df = pd.concat([object_outage_table_df, object_row_parsed_df], axis=0)
        
                st.table(object_outage_table_df)    

        # Show warnings if any are detected
        if len(warning_share_msgs) > 0:
            st.markdown("")
            st.markdown("#### ⚠️ Warnings")
            st.caption("We found the following warnings on the shares below. Once you adjust your pipelines to the new state of these shares, feel free to reset the monitor status in the All Shares section below.")

            # Show details of each warning
            for msg in warning_share_msgs:
                st.markdown("**Database** " + msg["SP_ARGUMENTS"])
                full_warning_message = msg["SP_EXEC_MSG"]
                full_warning_list = full_warning_message.splitlines()
                st.warning(full_warning_list[0])
                full_warning_list.remove(full_warning_list[0])
                full_warning_list.remove(full_warning_list[0])
                object_warning_table_df = pd.DataFrame(columns=['Object Type','Object Name'])
                
                for object_row in full_warning_list:
                    object_row_list = object_row.split(",")
                    object_type = object_row_list[0].split(": ")[1]
                    object_name = object_row_list[1].split(": ")[1]
                    object_row_parsed = [object_type, object_name]
                    object_row_parsed_df = pd.DataFrame(columns=object_warning_table_df.columns, data=[object_row_parsed])
                    object_warning_table_df = pd.concat([object_warning_table_df, object_row_parsed_df], axis=0)
        
                st.table(object_warning_table_df) 
                
        # Select share to show its history 
        st.markdown("#### All Shares")
        tracked_databases_df = session.table("SHAREWATCH.UTIL.DATABASES_MONITORED")
        tracked_databases_list = tracked_databases_df.select(col("DB_NAME")).distinct().to_pandas()
        tracked_database_option = st.selectbox(
           "Select a database to view its history",
           tracked_databases_list
        )

        # Create columns for layout
        all_shares_button_col1, all_shares_button_col2, all_shares_button_col3 = st.columns([12.75,7,6.5])
        check_history_button = all_shares_button_col2.button("Check Monitor History")

        # If the check history button is clicked, display the monitor history for the selected database
        if check_history_button:
            runlog_df = session.table("SHAREWATCH.UTIL.SP_RUNLOG")
            db_logs_df = runlog_df.filter(col("SP_ARGUMENTS")== tracked_database_option)
            ordered_db_logs_df = db_logs_df.select(col("SP_EXEC_TIME"), col("SP_EXEC_MSG")).sort(col("SP_EXEC_TIME").desc())

            latest_timestamp = datetime.strptime('1970-01-01',"%Y-%m-%d")
            latest_message = ''

            st.table(ordered_db_logs_df)

        
        # If the reset button is clicked, reset the monitor status for the selected database
        reset_button = all_shares_button_col3.button("Reset Monitor Status")
        if reset_button:
            with st.spinner("Resetting share monitor..."):
                session.sql("call sharewatch.util.drop_schemadrift_monitor('" + str(tracked_database_option) + "');").collect()
                session.sql("call sharewatch.util.setup_schemadrift_monitor('" + str(tracked_database_option) + "');").collect()
                session.sql("call sharewatch.util.run_schemadrift_monitor()").collect()
                st.info("Share monitor has been reset successfully! Please refresh the page to see the latest alerts...")
            

    with configuration_tab:
        # Show section title and introduction
        st.markdown("# ShareWatch")
        st.markdown(
            "ShareWatch is part of a set of best practices to help data consumers operationalize their shares into their mission-critical data pipelines and mitigate the effects of schema drift. You can read **the full whitepaper here**."
        )
        st.divider()
        
        st.markdown("## Configuration")       
        st.markdown("#### Configure App")
        
        # Pull current email configuration and set it to the text input
        st.markdown("###### Set Email Alert Receipient")
        app_config_df = session.table("SHAREWATCH.UTIL.APP_CONFIG")
        email_config_df = app_config_df.filter(col("PARAMETER").substring(0,18)=='NOTIFICATION_EMAIL').collect()
        current_email = json.loads(email_config_df[0][1]).get("email_address")
        email_config = st.text_input('Add an email address to receive alerts when drift occurs', value=current_email, key='email_config')
    
        email_config_button_col1, email_config_button_col2 = st.columns([12.65,3.5])
        email_config_button = email_config_button_col2.button("Set Alerting Email")

        # If the Set Alerting Email button is clicked, set the alert email recipient
        if email_config_button:
            if len(email_config) == 0 or '@' not in email_config:
                st.error('Please add a valid email address that you would like the alert emails to go to...')
            else:
                with st.spinner("Setting alert email address..."):
                    session.sql("call sharewatch.util.update_app_config(\'notification_email\',\'{\"email_address\": \"" + str(email_config) + "\"}\');").collect()
                    st.success("Alert email address set to: " + email_config)
                    
        
        st.markdown("###### Set Task Schedule")
        scheduler_col1, scheduler_col2 = st.columns([2,2])
        
        # Pull current task schedule and set it to the frequency dropdowns
        app_config_df = session.table("SHAREWATCH.UTIL.APP_CONFIG")
        task_schedule_config_df = app_config_df.filter(col("PARAMETER").substring(0,18)=='MASTER_TASK').collect()
        task_schedule = json.loads(task_schedule_config_df[0][1]).get("schedule")
        task_schedule_min = 0 if str(task_schedule)[1]==' ' else str(task_schedule)[2]
        task_schedule_hour = 0 if str(task_schedule)[str(task_schedule).find('*', str(task_schedule).find('*') + 1) + 1] == ' ' else str(task_schedule)[str(task_schedule).find('*', str(task_schedule).find('*') + 1) + 2]
        
        frequency_hour = scheduler_col1.selectbox('Run Frequency', index=int(task_schedule_hour), key='frequency_hour', options=('0h','1h','2h','3h','4h','5h','6h','7h','8h','9h','10h','11h','12h','13h','14h','15h','16h','17h','18h','19h','20h','21h','22h','23h'))
        frequency_minute = scheduler_col2.selectbox('', index=int(task_schedule_min), key='frequency_minute', options=('0m','1m','2m','3m','4m','5m','6m','7m','8m','9m','10m','11m','12m','13m','14m','15m','16m','17m','18m','19m','20m','21m','22m','23m','24m','25m','26m','27m','28m','29m','30m','31m','32m','33m','34m','35m','36m','37m','38m','39m','40m','41m','42m','43m','44m','45m','46m','47m','48m','49m','50m','51m','52m','53m','54m','55m','56m','57m','58m','59m'))
        frequency_hour_num = int(str(frequency_hour).replace('h',''))
        frequency_minute_num = int(str(frequency_minute).replace('m',''))
        
        set_schedule_button_col1, set_schedule_button_col2 = st.columns([12.35,3.5])
        set_schedule_button = set_schedule_button_col2.button("Set Task Schedule")

        # If the Set Task Schedule button is clicked, set task based on translated cron input
        if set_schedule_button:
            if frequency_hour_num == 0 and frequency_minute_num < 5:
                st.error('Please choose a run frequency with a minimum of 0h 5m.')
            else:
                with st.spinner("Setting task schedule based on chosen run frequency..."):
                    cron_hour_wildcard_structure = '*' if frequency_hour_num == 0 else '*/' + str(frequency_hour_num)
                    cron_minute_wildcard_structure = '*' if frequency_minute_num == 0 else '*/' + str(frequency_minute_num)
                    frequency_in_cron = cron_minute_wildcard_structure + ' ' + cron_hour_wildcard_structure + ' * * *'
    
                    session.sql("call sharewatch.util.configure_master_task('" + str(frequency_in_cron) + " UTC')").collect()                    
                    st.success("✅ Task rescheduled successfully!")

        st.markdown("#### Configure Databases")

        # Pull all databases for selection
        st.markdown("###### Add Databases")
        all_databases_df = pd.DataFrame(session.sql("SHOW DATABASES").collect())
        all_shares_df = all_databases_df[all_databases_df.kind=="IMPORTED DATABASE"]    
        shares_to_add = st.multiselect(
                label="Select share(s) to add",
                options=all_shares_df["name"]
        )
        
        add_button_col1, add_button_col2 = st.columns([15.1,3])
        add_monitors_button = add_button_col2.button("Add " + ("Monitor" if len(shares_to_add) == 1 else "Monitors"))

        # If the Set Task Schedule button is clicked, set task run frequency based on translated cron input
        if add_monitors_button:
            if len(shares_to_add) < 1:
                st.error('Please select a share to add a monitor for.')
            else:
                share_counter = 0
                with st.spinner("Adding monitors..."):
                    for share_to_add in shares_to_add:
                        session.sql("call sharewatch.util.setup_schemadrift_monitor('" + share_to_add + "')").collect()
                        session.sql("call sharewatch.util.run_schemadrift_monitor('" + share_to_add + "')").collect()
                        share_counter = share_counter + 1
                    st.success("✅ " + str(share_counter) + (" monitor " if share_counter == 1 else " monitors ") + "successfully added to track!")
        
        # Show currently monitored databases
        st.markdown("###### Remove Databases")
        raw_databases_monitored_df = session.table("SHAREWATCH.UTIL.DATABASES_MONITORED").select(col('DB_NAME')).distinct()
        selectable_databases_tracked_df = raw_databases_monitored_df.with_column('Select', lit(False))
        databases_monitored_df = selectable_databases_tracked_df.select(col("SELECT"), col("DB_NAME"))

        if 'databases_monitored_df' not in st.session_state:
            st.session_state['databases_monitored_df'] = databases_monitored_df

        st.session_state["databases_monitored_df"] = st.experimental_data_editor(st.session_state["databases_monitored_df"])

        shares_to_remove = []
        for select_rownum, select_row in enumerate(st.session_state['databases_monitored_df']["SELECT"]):
            for dbname_rownum, dbname_row in enumerate(st.session_state['databases_monitored_df']["DB_NAME"]):
                if select_rownum == dbname_rownum:
                    if select_row == True:
                        shares_to_remove.append(dbname_row)

        remove_button_col1, remove_button_col2 = st.columns([10.75,3])
        remove_monitors_button = remove_button_col2.button("Remove " + ("Monitor" if len(shares_to_remove) == 1 else "Monitors"))

        # If the Remove Monitors button is clicked, remove the monitor from the ShareWatch monitors list
        if remove_monitors_button:
            if len(shares_to_remove) < 1:
                st.error('Please select a share to remove a monitor for.')
            else:
                with st.spinner("Removing monitors..."):
                    remove_share_counter = 0
                    for share_to_remove in shares_to_remove:
                        session.sql("call sharewatch.util.drop_schemadrift_monitor('" + share_to_remove + "')").collect()
                        remove_share_counter = remove_share_counter + 1
                    st.success("✅ " + str(remove_share_counter) + (" monitor " if remove_share_counter == 1 else " monitors ") + "successfully removed!")