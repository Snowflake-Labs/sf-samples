
from flask import Flask, request, Response, jsonify
import logging
import os

import re

app = Flask(__name__)

from langchain.base_language import BaseLanguageModel

from langchain_core.language_models.llms import LLM
from typing import Any, List, Mapping, Optional
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models.llms import LLM

from nemoguardrails import LLMRails, RailsConfig
from nemoguardrails.llm.providers import register_llm_provider

# # Connect to Snowflake 
from snowflake.snowpark.session import Session






class SnowflakeCortexLLM(LLM,extra='allow'):
    def __init__(self):
        super().__init__()
        self.sp_session = Session.builder.configs({
                            "host":os.getenv('SNOWFLAKE_HOST'),
                            "account":os.getenv('SNOWFLAKE_ACCOUNT'),
                            "token": self.get_login_token(),
                            "authenticator":'oauth',
                            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE')
                            }).create()


    model: str = os.getenv('MODEL')
    '''The Snowflake cortex hosted LLM model name. Defaulted to :Mistral-large Refer to doc for other options. '''

    cortex_function: str = 'complete'
    '''The cortex function to use, defaulted to complete. for other types refer to doc'''

    @property
    def _llm_type(self) -> str:
        return "snowflake_cortex"

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        
        prompt_text = prompt
        sql_stmt = f'''
            select snowflake.cortex.{self.cortex_function}(
                '{self.model}'
                ,'{prompt_text}') as llm_reponse;'''
        
        l_rows = self.sp_session.sql(sql_stmt).collect()

        llm_response = l_rows[0]['LLM_REPONSE']

        return llm_response

    @property
    def _identifying_params(self) -> Mapping[str, Any]:
        """Get the identifying parameters."""
        return {
            "model": self.model
            ,"cortex_function" : self.cortex_function
            ,"snowpark_session": self.sp_session.session_id
        }
    
    def get_login_token(self):
        with open('/snowflake/session/token', 'r') as f:
            return f.read()

    
register_llm_provider("snowflake_cortex", SnowflakeCortexLLM)
config = RailsConfig.from_path("./nemo-config/")
nemoguard_app = LLMRails(config)

def extract_json_from_string(s):
    logging.info(f"Extracting JSON from string: {s}")
    # Use a regular expression to find a JSON-like string
    matches = re.findall(r"\{[^{}]*\}", s)

    if matches:
        # Return the first match (assuming there's only one JSON object embedded)
        return matches[0]

    # Return the original string if no JSON object is found
    return s


@app.route("/", methods=["POST"])
def udf():
    try:
        request_data: dict = request.get_json(force=True)  # type: ignore
        return_data = []
        print(request_data)
        for index, col1 in request_data["data"]:
            completion = nemoguard_app.generate(messages=[{
                        "role": "user",
                        "content": col1
                    }])
            return_data.append(
                [index, extract_json_from_string(completion['content'])]
            )

        return jsonify({"data": return_data})
    except Exception as e:
        app.logger.exception(e)
        return jsonify(str(e)), 500
