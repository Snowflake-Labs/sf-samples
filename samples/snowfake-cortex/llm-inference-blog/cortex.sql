USE ROLE ACCOUNTADMIN;

-- Translate

select snowflake.cortex.translate('wie geht es dir heute?','de_DE','en_XX');

select transcript,snowflake.cortex.translate(transcript,'de_DE','en_XX')
from call_transcripts 
where language = 'German';

-- Sentiment Score

select transcript, snowflake.cortex.sentiment(transcript) 
from call_transcripts 
where language = 'English';

-- Summarize

select transcript,snowflake.cortex.summarize(transcript) 
from call_transcripts 
where language = 'English' limit 1;

-- Prompt Engineering

SET prompt = 
'### 
Summarize this transcript in less than 200 words. 
Put the product name, defect and summary in JSON format. 
###';

select snowflake.cortex.complete('llama2-70b-chat',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

-- Other LLMs

select snowflake.cortex.complete('mixtral-8x7b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('mistral-7b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

select snowflake.cortex.complete('gemma-7b',concat('[INST]',$prompt,transcript,'[/INST]')) as summary
from call_transcripts where language = 'English' limit 1;

