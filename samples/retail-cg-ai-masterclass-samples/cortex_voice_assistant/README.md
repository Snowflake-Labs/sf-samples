#Cortex Voice Assistant

Cortex Voice Assistant is a Streamlit app that allows users to interact with a voice-based AI assistant powered by OpenAI's Whisper ASR and Snowflake Cortex LLM functions. The assistant uses Whisper model to transcribe user questions from speech to text, and Google gTTS to generate speech from text. It uses an LLM from Snowflake Cortex to convert a question from simple English to a SQL query. The app integrates with Snowflake to execute SQL queries based on user voice input.

##Prerequisites

Before running the Cortex Voice Assistant app, ensure you have the following:
- Python 3.7 or higher installed
- Anaconda or Miniconda installed
- OpenAI API key

##Environment Setup

To set up a clean Snowpark conda environment for running the app, follow these steps:

1. Open a terminal or command prompt and create a new folder called llm-chatbot.
2. Create a new conda environment named snowpark-llm-chatbot with Python 3.10:
'''
	conda create --name snowpark-llm-chatbot python=3.10
'''
3. Activate the newly created environment:
'''
	conda activate snowpark-llm-chatbot
'''

4. Install the required packages:
'''
	conda install snowflake-snowpark-python "openai>=1.0.0"
	conda install conda-forge::"streamlit>=1.28.2"
	pip install speechrecognition pyttsx3 gTTS pygame
'''

5. Install PyAudio:
	On Windows machines simply use
'''
	pip install PyAudio
'''
	
	On Macbooks first install the xxxx library with brew. So you should install with the following commands:
'''
	brew install portaudio
	pip install PyAudio
'''

## Create Database assets
Run the following SQL commands in a SQL worksheet to create the warehouse, database and schema.

'''
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE ML_HOL_WH; --by default, this creates an XS Standard Warehouse
CREATE OR REPLACE DATABASE ML_HOL_DB;
CREATE OR REPLACE SCHEMA ML_HOL_SCHEMA;
CREATE OR REPLACE STAGE ML_HOL_ASSETS; --to store model assets

-- create csv format
CREATE FILE FORMAT IF NOT EXISTS ML_HOL_DB.ML_HOL_SCHEMA.CSVFORMAT 
    SKIP_HEADER = 1 
    TYPE = 'CSV';

-- create external stage with the csv format to stage the diamonds dataset
CREATE STAGE IF NOT EXISTS ML_HOL_DB.ML_HOL_SCHEMA.DIAMONDS_ASSETS 
    FILE_FORMAT = ML_HOL_DB.ML_HOL_SCHEMA.CSVFORMAT 
    URL = 's3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv';
    -- https://sfquickstarts.s3.us-west-1.amazonaws.com/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv

LS @DIAMONDS_ASSETS;

CREATE TABLE IF NOT EXISTS DIAMONDS (
    CARAT   NUMBER(10,3),
    CUT     VARCHAR,
    COLOR   VARCHAR,
    CLARITY VARCHAR,
    DEPTH   NUMBER(10,2),
    TABLE_PCT   NUMBER(10,2),
    PRICE   NUMBER(20),    
    X       NUMBER(10,2),
    Y       NUMBER(10,2),
    Z       NUMBER(10,2)
);

COPY INTO DIAMONDS
  FROM @DIAMONDS_ASSETS
  FILE_FORMAT = (FORMAT_NAME = 'CSVFORMAT')
  ON_ERROR = CONTINUE;  
    
SELECT * FROM DIAMONDS LIMIT 100; 
'''
These can also be found in the setup.sql file.

##Running the App

1. Clone the repository:
'''
git clone https://github.com/your-username/cortex-voice-assistant.git
'''

2. Navigate to the project directory:
'''
cd llm-chatbot
'''

3. Set your OpenAI API key and database credentials:
	3.1 In the folder, llm-chatbot, create a new folder called '.streamlit'
	'''
		mkdir .streamlit
	'''
	
	3.2 Navigate to the new folder:
	'''
		cd .streamlit
	'''
	
	3.3 In the .streamlit folder, create a new secrets.toml file that will contain your OpenAI API key and Snowflake credentials. The secrets.toml file should have the following format. Fill in the details with your OpenAI API key and Snowflake credentials: 
	'''
		# .streamlit/secrets.toml

		OPENAI_API_KEY = "sk-2v...X"

		[connections.snowflake]
		user = "<username>"
		password = "<password>"
		warehouse = "COMPUTE_WH"
		role = "ACCOUNTADMIN"
		account = "<account-id>"
	'''	

4. Run the Streamlit app:
'''
streamlit run cortex_voice_assistant.py
'''

5. Open a web browser and navigate to http://localhost:8501 to access the Cortex Voice Assistant app.


##Usage
1. Grant microphone access when prompted by the browser.
2. Click the microphone button and speak your question or command.
3. The app will transcribe your speech using OpenAI's Whisper ASR and generate a response using Google's gTTS.
4. The app also uses the Cortex Complete LLM function for basic text-to-SQL generation
5. If the response contains a valid SQL query, the app will execute it against the connected Snowflake database and display the results.
6. You can ask questions such as:
   	Show the 5 most expensive diamonds
   	Which 3 diamonds have the highest carat values?
   	Show the average price by cut and sort the results by average price in descending order
   	Show the average price by color and sort the results by average price
	Is there a correlation between price and carat? Use the corr function
7. To exit the app, say "thank you" or close the browser tab.

##Troubleshooting

- If you encounter any issues with package installations or environment setup, ensure you have the latest versions of Anaconda or Miniconda installed.
- Make sure your OpenAI API key is valid and properly set as an environment variable.
- If the app fails to start or crashes, check the terminal output for any error messages and ensure all required packages are installed correctly.

##Contributing
Contributions to the Cortex Voice Assistant app are welcome! If you find any bugs or have suggestions for improvements, please open an issue or submit a pull request on the GitHub repository.

##License
This project is licensed under the Apache Version 2.0 License.
