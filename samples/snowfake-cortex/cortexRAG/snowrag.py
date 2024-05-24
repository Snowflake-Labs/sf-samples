from snowflakeLM import SnowflakeLM
from snowretriever import SnowflakeRM

class SnowRAG():

    def __init__(self,
                 embeddings_table: str,
                 lm_model: str = "mixtral-8x7b",
                 k: int = 3,
                 snowflake_session=None,
                 prompt_template:str = None,
                 **kwargs):
        
        self.LM = SnowflakeLM(model=lm_model,snowflake_session=snowflake_session)
        self.RM = SnowflakeRM(snowflake_table_name=embeddings_table,snowflake_session=snowflake_session,k=k)
        self.prompt_template = prompt_template

    def _zero_shot_rag(self,prompt):

        passages = self.RM.forward(prompt)

        if self.prompt_template is None:
            template = f"""Answer the question with short factoid answers:
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

        