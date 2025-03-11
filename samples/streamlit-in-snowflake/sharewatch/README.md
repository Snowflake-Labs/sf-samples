# ShareWatch QuickStart

## Overview
ShareWatch is a Streamlit-based application installed on consumers of data share's Snowflake accounts to detect schema drifts in data shares received by these consumers.  ShareWatch triggers notifications based on schema drift detection, which is part of Snowflake's [best practices to operationalize shares](https://docs.google.com/document/d/1PAAw9I_BXwOV8hxPYrWHT5ubbZjUt6t43KWKZK8Kefk/edit), and integrates them in mission-critical data pipelines. The app provides a starting point and can be extended to implemenet other best practices discussed in the whitepaper. 

Schema drift occurs where the structure (schema) of shared data is changed over time by the data provider. ShareWatch detects schema drifts by comparing the expected schema of each share to its current schema, and displaying the state of each share monitored in the app. ShareWatch also sets up a master task that runs all monitors on a schedule and sends email notifications if:
1. Share is in "Warning" State: schema is appended i.e. new schema, object, or attribute is added to the schema. 
2. Share is in "Outage" State: schema is altered i.e. An alter, rename, delete, or revoke command on a schema, object, or attribute is performed

![alt text](Monitoring_vs_Configuration_Example.png)

ShareWatch will also tell you which schemas, objects, or attributes are affected so you have a targeted area to explore and make the appropriate fixes in your pipelines. ShareWatch's "Accept Drift" feature allows you to accept changes in a share's schema and make it the new "expected" schema.

See below diagram for details on how ShareWatch detects schema drift 

![alt text](Explanation.png)

## Key Features
Share Watch has 2 sections
A) Configuration section - Set up schema drift monitoring on databases mounted from shares, set up an email recepient for notifications, and set up frequency at which ShareWatch's task should check for schema drift.
B) Monitoring section - Shows the status of schema drift on databases mounted from shares based on the monitors set up in the Configuration section.

## Prerequisites
- AccountAdmin role access in your Snowflake account (or a Snowflake trial account)
- One or more data shares set up from Marketplace or Private Listings ([here's a guide for setting up shares](https://www.youtube.com/watch?v=4ksdOBT1HCw))
- An email you can send schema drift alerts to






# Install ShareWatch
- Create a SQL worksheet in Snowsight and paste the ShareWatch SQL Setup script from sf-samples. (LINK TO SQL SCRIPT)
- Select all commands, and run them all. This will set up all the stored procedures used by the ShareWatch app. 

- Create a Streamlit app in Snowsight and replace the starter code it comes with with the ShareWatch Streamlit script. (LINK TO STREAMLIT SCRIPT)
  - Choose an appropriate warehouse and a database to set your Snowflake internal stage that would hold your Streamlit's python files.
- Refresh the app after pasting the ShareWatch Streamlit script and (assuming you've already set up the stored procedures from the SQL script), you should now see the Initial Configuration section in your Streamlit app. 
- At this point, you're all set! You can hide the code and start using the Streamlit app as intended, proceeding to the Initial Configuration section.

# Initial Configuration
- The Initial Configuration section includes 3 pieces to set up:
  - Which shares do you want to set up monitors for? (You can always add/remove shares after this step too in the Configuration section)
  - What email recipient do you want the alerts sent to?
  - What schedule do you want to set up the ShareWatch Task with? 
    - ShareWatch will create the task in a suspended state, assuming you want to start your schedule from the moment you initiate this configuration.
- Once you have the settings above configured to your liking, you can click the Add Shares button.
  - Be careful that this task will initiate a warehouse and could incur costs
  - This will create a database called SharewWatch
    - Utils schema
    - For each monitor, it

![alt text](Initial_Configuration_Filled.png)




# Monitoring Shares
## What Triggers Alerts
ShareWatch's monitors are run from two places: either from task based on the schedule you set up or manually from Run Monitors button at the top right of the Monitoring section.

When a monitor runs, it will check the expected schema of each share against the current schema of the share. If no changes are found, it will log in the share's history that it was checked at this time and skip to the next one.

If it does find drift within the share's schema, it will trigger one of two warning depending on the type of drift:
1. Share is in "Warning" State: schema is appended i.e. new schema, object, or attribute is added to the schema. 

![alt text](Outage_Example.png)

2. Share is in "Outage" State: schema is altered i.e. An alter, rename, delete, or revoke command on a schema, object, or attribute is performed

![alt text](Warning_Example.png)

ShareWatch will also tell you which schemas, objects, or attributes are affected so you have a targeted area to explore and make the appropriate fixes in your pipelines.



## Share Monitor History
At the bottom of the Monitoring section, you can choose a check a share monitor to check its history, which includes:
- When the share monitor is set up
- When the share monitor is run to check for drift
- When the share monitor is triggered with a Warning
- When the share monitor is triggered with an Outage
- When the share monitor is removed

This history exists in the ShareWatch database and can be accessed from there for any workflows/analytics you'd like to build on top of ShareWatch.

![alt text](Share_History.png)

## Remediation
To reset a share monitor after drift is identified and it has been triggered with a Warning or an Outage, you can use the Reset Monitor Status button. This will temporarily remove and re-add the monitor again, essentially resetting its expected state.

![alt text](Remediation.png)

# Configuration Changes
## Changing Task Schedule
ShareWatch will initially create the task in a suspended state as part of the Initial Configuration stage, assuming you want to start your schedule from the moment you initiate this configuration. 

Here, you can change that task's schedule and it will start that schedule from the moment you changed this configuration as well.

You can always check the current configuration of the task using: describe task sharewatch.util.master_task;

## Removing Share Monitors
This will show you the current shares you already have monitors for. If you want to remove any of them to reduce the number of shares checked every cadence of the task, you can do it from here. This will keep the history of that share if it gets added again.

## Adding Share Monitors
This will show you the current databases based on shares you have in your environment. You can pick any of these shares to add a monitor for, and this share monitor will then be included in any future runs of the master task checking for schema drift in this share's database. If this share was added before as a monitor and then removed, you will also be able to see the history of that share under the Share History section.

![alt text](Configuration.png)