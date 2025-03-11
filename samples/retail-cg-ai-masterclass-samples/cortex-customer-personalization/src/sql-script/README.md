# DCR Setup for Ad Campaign Optimization
Extensions to the DCR 5.5 framework to support Ad Campaign Optimization use case

Initial testing has been performed between DCR1 (ana95816) and DCR (pqa94088)


1. Install DCR 5.5 framework


	- Install Quickstart here: https://quickstarts.snowflake.com/guide/build_a_data_clean_room_in_snowflake_advanced/index.html?index=..%2F..index#3

	- Use Prefix = AdCampaignOpt

	- Use the Streamlit Setup UI script to customize naming conventions in the DCR
	
		https://snowflake-labs-sfquickstart-data-cle-dcr-setup-assistant-bkx7gg.streamlit.app/

2. Install UI Dependencies
	- Python 
	- Streamlit
	- Snowflake Python Connector

3. Setup/Populate first party data in each account
  
	Provider (DCR1) 
	Execute sql scripts in Snowsight:
	- 2.1-provider_templates_AdCampaignOpt.sql 	//Populate query template
	- 2.2-provider_data.sql	 				//Generate test data sets

		Produces: DCR_AdCampaignOpt_Provider_DB.Cleanroom.Provider_Impressions_vw
		
		(Refers to table DCR_AdCampaignOpt_Provider_DB.Shared_Schema.Impressions)
	

	Consumer(DCR2)
		Execute sql scripts in Snowsight:
 	- 5-consumer_data.sql 					(Generate test data set)

 		Produces: DCR_AdCampaignOpt_Consumer.mydata.customers;
 

4. Add demo execution example in Consumer Account (DCR2) 

	Execute sql script in Snowsight:
	- 7.1-consumer_request_AdCampaignOpt.sql	 	(Test query executions for demo)

5. Try demo execution example using Streamlit UI
	
- Edit <ui install dir>/.streamlit/secrets.toml and populate the values for 
```sh
	user = <snowflake user>
        password = <snowflake user password>
        account = "PQA94088"
        warehouse = "APP_WH"
        database = "DCR_ADCAMPAIGNOPT_APP"
        schema = "CLEANROOM"
        role = "DATA_CLEAN_ROOM_ROLE"	
```
	
- Open a terminal window
```sh
        $ cd <directory where ui is installed>
        $ streamlit run dcr_adcampaignopt_ui.py	
  ```
  - Select template: ad_campaign_opt_4way
  - Select any set of dimensions listed in the dropdown
  -  Provide a filter (WHERE) clause (optional but recommended to limit size of result)
        Ex: “seqnum <= 100”
  - Click SUBMIT
  - Results will be retrieved to:
  	- DCR_ADCAMPAIGNOPT_CONSUMER.RESULTS.ad_campaign_opt_4way_RESULT
	- But also display on screen in tabular form	
