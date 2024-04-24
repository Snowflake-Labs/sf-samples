from openai import OpenAI

import re
import streamlit as st
from prompts_diamonds_cortex import get_system_prompt
import speech_recognition as sr
import pyttsx3
import time
from gtts import gTTS
from io import BytesIO
import pygame
from pygame import mixer

client = OpenAI(api_key=st.secrets.OPENAI_API_KEY)

def whisper(audio):
    with open('speech.wav','wb') as f: 
        f.write(audio.get_wav_data())
    speech = open('speech.wav', 'rb')

    wcompletion = client.audio.transcriptions.create(model = "whisper-1",
    file=speech,
    language="en")

    transcribed_input = wcompletion.text
    return transcribed_input

def speak(text, language='en', tld='com'):
    """
    Generate an MP3 byte stream from the given text using Google Text-to-Speech (gTTS).

    Args:
        text (str): The text to convert to speech.
        language (str): The language code for the speech (default: 'en').
        tld (str): The top-level domain for the Google Translate host (default: 'com').

    Returns:
        BytesIO: A BytesIO object containing the MP3 data.
    """
    # Create a BytesIO object to store the MP3 data
    mp3_buffer = BytesIO()

    try:
        # Initialize the gTTS object with the given parameters
        tts = gTTS(text, lang=language, tld=tld)
        
        # Write the MP3 data directly to the BytesIO buffer
        # This avoids creating an intermediate file on disk
        tts.write_to_fp(mp3_buffer)
        
        # Move the buffer's read position to the beginning
        mp3_buffer.seek(0)
        
        return mp3_buffer
    
    except Exception as e:
        # Log any exceptions that occur during the TTS process
        print(f"An error occurred during text-to-speech conversion: {str(e)}")
        
        # Close the BytesIO buffer and return None to indicate an error
        mp3_buffer.close()
        return None

def play_sound(sound, audio_format="mp3", sleep_time=3):
    sound.seek(0)
    mixer.music.load(sound, audio_format)
    mixer.music.play()
    time.sleep(sleep_time)

usewhisper = True
first_greeting = False

st.title("☃️ Snowflake Cortex Assistant")
st.cache_data(show_spinner="Loading Cortex Assistant's context...")
conn = st.connection("snowflake")

df_system_prompt = 'Hi! I\'m your Snowflake Cortex Assistant. How can I help you today?'

# Initialize the chat messages history
if "messages" not in st.session_state:
    # system prompt includes table information, rules, and prompts the LLM to produce
    # a welcome message to the user.
    st.session_state.messages = [{"role": "system", "content": df_system_prompt}]

# speech recognition set-up
r = sr.Recognizer()
mic = sr.Microphone()
r.energy_threshold = 400

# Display chat messages from history on app rerun
for message in st.session_state.messages:        
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if first_greeting == False:
    pygame.init()
    mixer.init()
    sound = speak('Hi! I\'m your Snowflake Cortex Assistant. How can I help you today?', 'en', 'co.uk')   

    play_sound(sound, 'mp3', 3)
    first_greeting = True
    
toggle = 1
# Prompt for user input and save
while True:  
    with mic as source:
        if first_greeting == False:
            mixer.init()
            if toggle == 1:
                sound = speak('What else can I do for you today?', 'en', 'co.uk')   
                toggle += 1
            elif toggle == 2:
                sound = speak('Do you have any other question for me?', 'en', 'co.uk')
                toggle += 1
            else:
                sound = speak('Is there anything else I can help with?', 'en', 'co.uk')
                toggle = 1

            play_sound(sound, 'mp3', 3)
              
        first_greeting = False

        print("\nListening...")

        r.adjust_for_ambient_noise(source, duration = 2)
        audio = r.listen(source)       

        try:
            if usewhisper:
                prompt = whisper(audio)
                #print("Response from Whisper:")
                print("prompt = ", prompt)
            else:
                print("Speech recognition with Google ASR...")
                prompt = r.recognize_google(audio, language='en-US')
                #print("Response from Google ASR:")
                print("prompt = ", prompt)
        except:
            print("Sorry, I could not Understand You!")
            continue 

    if "thank you" in prompt.lower():
        sound = speak('You\'re very welcome. Bye!', 'en', 'co.uk')   
        play_sound(sound, 'mp3', 3)
        break;

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):   	
        response = ""
        message_placeholder = st.empty()
        resp_container = st.empty()
        sql_stmt = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', CONCAT($${get_system_prompt()}$$, 'Question:  ', '{prompt}', 'Answer: the most important piece of information is the SQL QUERY. BE CONCISE AND JUST SHOW THE SQL QUERY. DO NOT SHOW ANY TEXT AFTER THE SQL QUERY!')) as response"
        df = conn.query(sql_stmt)
        full_response = df.iloc[0]['RESPONSE']
        message_placeholder.markdown(full_response)
        message = {"role": "assistant", "content": full_response}

        # Parse the response for a SQL query and execute if available
        sql_match = re.search(r"```sql\n(.*)\n```", full_response, re.DOTALL)
        if sql_match:
            try:
                sql = sql_match.group(1)
                message["results"] = conn.query(sql)
                st.dataframe(message["results"])
            except:
                st.write("Invalid query. Try another question.")
                print("Invalid query. Try another question.")
                
        st.session_state.messages.append(message)       

