"""Module for interacting with Snowflake Cortex."""
import json
from typing import Any

try:
    from snowflake.snowpark import Session
    from snowflake.snowpark import functions as snow_func

except ImportError:
    pass


class SnowflakeLM():
    """Wrapper around Snowflake's CortexAPI.

    Currently supported models include 'snowflake-arctic','mistral-large','reka-flash','mixtral-8x7b',
    'llama2-70b-chat','mistral-7b','gemma-7b','llama3-8b','llama3-70b','reka-core'.
    """

    def __init__(self, model: str = "mixtral-8x7b", snowflake_session=None, **kwargs):
        """Parameters

        ----------
        model : str
            Which pre-trained model from Snowflake to use?
            Choices are 'snowflake-arctic','mistral-large','reka-flash','mixtral-8x7b','llama2-70b-chat','mistral-7b','gemma-7b'
            Full list of supported models is available here: https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#complete
        credentials: dict
            Snowflake credentials required to initialize the session. 
            Full list of requirements can be found here: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.Session
        **kwargs: dict
            Additional arguments to pass to the API provider.
        """

        self.model = model
        self.available_args = {
                "max_tokens",
                "temperature",
                "top_p",
            }
        self.client = self._init_cortex(snowflake_session=snowflake_session)
        self.history: list[dict[str, Any]] = []
        self.kwargs = {
            "temperature": 0.7,
            "max_output_tokens": 1024,
            "top_p": 1.0,
            "top_k": 1,
            **kwargs,
        }

    def _init_cortex(self,snowflake_session: object):

        snowflake_session.query_tag = {"origin": "sf_sit", "name": "xom", "version": {"major": 1, "minor": 0}}

        return snowflake_session

    def _prepare_params(
        self,
        parameters: Any,
    ) -> dict:
        params_mapping = {"n": "candidate_count", "max_tokens": "max_output_tokens"}
        params = {params_mapping.get(k, k): v for k, v in parameters.items()}
        params = {**self.kwargs, **params}
        return {k: params[k] for k in set(params.keys()) & self.available_args}

    def _cortex_complete_request(self, prompt: str, **kwargs) -> dict:
        
        complete = snow_func.builtin("snowflake.cortex.complete")
        cortex_complete_args = complete(
            snow_func.lit(self.model),
            snow_func.lit([{"role": "user", "content": prompt}]),
            snow_func.lit(kwargs),
        )
        
        response = self.client.range(1).withColumn("complete_cal", cortex_complete_args).collect()[0].COMPLETE_CAL

        return json.loads(response)

    def basic_request(self, prompt: str, **kwargs) -> list:
        raw_kwargs = kwargs
        kwargs = self._prepare_params(raw_kwargs)

        response = self._cortex_complete_request(prompt, **kwargs)

        history = {
            "prompt": prompt,
            "response": {
                "prompt": prompt,
                "choices": [{"text": c} for c in response["choices"]],
            },
            "kwargs": kwargs,
            "raw_kwargs": raw_kwargs,
        }

        self.history.append(history)

        return [i["text"]["messages"] for i in history["response"]["choices"]]


    def _request(self, prompt: str, **kwargs):
        """Handles retrieval of completions from Snowflake Cortex whilst handling API errors."""
        return self.basic_request(prompt, **kwargs)

    def __call__(
        self,
        prompt: str,
        **kwargs,
    ):
        return self._request(prompt, **kwargs)
