from snowflakeLM import SnowflakeLM
from snowretriever import SnowflakeRM

class SnowRAG():
    """Simple Zero Shot RAG pipeline that uses Snowlfake Cortex LLMs and Snowflake embeddings

    Assumes Snowflake embeddings table contains a column with the 
    relevant passages (w/ column name CHUNK) and a column with the associated embeddings (w/ column name CHUNK_VEC)
     
    Args:
        embeddings_table (str): name of the Snowflake table containing the embeddings
        lm_model (str): name of the Snowflake Cortex language model to use
        snowflake_session (obj): authenticated session object
        k (int, optional): number of passages to retrieve from the embeddings table. Defaults to 3.
        embeddings_field (str,optional): name of the Snowflake table column containing the embeddings. Defaults to CHUNK_VEC
        embeddings_passage_field (str,optional): name of the Snowflake table column containing the passage associated with the embeddings. Defaults to CHUNK
        prompt_template (str, optional): prompt template to use in the RAG pipeline. Defaults to a Chain of Thought approach.
    """

    def __init__(self,
                 embeddings_table: str,
                 snowflake_session: object,
                 lm_model: str = "mixtral-8x7b",
                 k: int = 3,
                 embeddings_model = "e5-base-v2",
                 embeddings_field: str = "CHUNK_VEC",
                 embeddings_text_field: str = "CHUNK",
                 prompt_template:str = None,
                 **kwargs):
        
        self.LM = SnowflakeLM(model=lm_model,snowflake_session=snowflake_session)
        self.RM = SnowflakeRM(snowflake_table_name=embeddings_table,
                              snowflake_session=snowflake_session,
                              embeddings_model=embeddings_model,
                              embeddings_field=embeddings_field ,
                              embeddings_text_field=embeddings_text_field, 
                              k=k)
        self.prompt_template = prompt_template

    def _zero_shot_rag(self,prompt):

        passages = self.RM.forward(prompt)

        if self.prompt_template is None:
            template = f"""Answer the question with short answers:
            ---
            ---
            Context:{passages}

            Reasoning: Let'sthink step by step in order to determine the answer to {prompt}.

            Answer: """
        
        else:
            template = self.prompt_template
    
        return self.LM(template)
    
    def __call__(
        self,
        prompt: str,
        **kwargs,
    ):
        return self._zero_shot_rag(prompt)

        