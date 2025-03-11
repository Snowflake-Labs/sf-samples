# This has to go first to suppress a warning relating to whisper
import warnings
warnings.filterwarnings("ignore", message=".*The 'nopython' keyword.*")

import curses
import glob
import openai
import os
import pyaudio
import shutil
import snowflake.connector
import sys
import time
import threading
import wave
import whisper

from datetime import datetime,timezone
from subprocess import DEVNULL, Popen, PIPE, STDOUT
from tabulate import tabulate


# Database/WH constants
WAREHOUSE_NAME = 'tjones_wh'
DATABASE_NAME = 'tjonesdb'
SCHEMA_NAME = 'summit_sch'
TABLE_NAME = 'landing_words_t'
ADVANCE_TABLE_NAME = 'advance_landing_words_t'
COMMON_WORDS_TABLE_NAME = 'common_words_t'
STREAM_NAME = 'landing_words_stream'
AD_WORDS_TABLE_NAME = 'ad_words_t'
WORD_COUNT_DT_NAME = 'word_aggregation_dt'
ADS_WORD_SCORE_DT_NAME = 'ad_results_t'

# Whisper constants
MODEL = whisper.load_model("base.en")

# Pyaudio constants
CHUNK = 4096
FORMAT = pyaudio.paInt16
CHANNELS = 1 if sys.platform == 'darwin' else 2
RATE = 44100
RECORD_SECONDS = 5
AUDIO_BASE_DIR = "./audio/"
AUDIO_RAW_DIR = "./audio_raw/"
START_RECORDING = False

# Curses constants
SCREEN_HEIGHT = 50
SCREEN_WIDTH = 180
WIN_HEIGHT = 30
WIN_WIDTH = 50
PANEL_START_Y = 5

# Other constants
WORDS_TO_IGNORE = ('', '.')
CHARS_TO_REPLACE= ('.', '!', ',', '|', '[', ']', '?')


def create_ads_table(con):
    """ Creates a set of tables relating to ads.
    """
    con.cursor().execute(f"create or replace database {DATABASE_NAME};")
    con.cursor().execute(f"create or replace schema {SCHEMA_NAME};")
    con.cursor().execute(f"create or replace table {TABLE_NAME}(user_id int, word varchar);")
    con.cursor().execute(f"create or replace table {ADVANCE_TABLE_NAME}(word varchar);")
    con.cursor().execute(f"create or replace table {COMMON_WORDS_TABLE_NAME}(word varchar);")
    con.cursor().execute(f"create or replace stream {STREAM_NAME} on table {TABLE_NAME};")
    con.cursor().execute(f"""
       create or replace table {AD_WORDS_TABLE_NAME}
       (ad_id int, target_word varchar, target_ad_name varchar, weight int);
       """)
    con.cursor().execute(f"""
       insert into {COMMON_WORDS_TABLE_NAME}
       values ('you'), ('what'), ('uh'), ('um'), ('me'),
       ('the'), ('this'), ('thing'), ('that'), ('be'),
       ('a'), ('of'), ('in'), ('is'), ('it'), ('have'),
       ('and'), ('it''s'), ('there'), ('there''s'), ('too'),
       ('i'), ('if'), ('to'), ('me'), ('was'), ('for'), ('get'), ('my'),
       ('oh'), ('something'), ('that''s'), ('do'), ('with'), ('we'),
       ('can'), ('do'), ('we'), ('with'), ('from'), ('so'), ('all'),
       ('are'), ('our'), ('should'), ('would'), ('like'), ('yeah'),
       ('mhm'), ('were'), ('we''ll'), ('no'), ('oh'), ('go'), ('okay'), ('but')
     """)
    con.cursor().execute(f"""
       insert into {AD_WORDS_TABLE_NAME}
       (ad_id, target_word, target_ad_name, weight) values
       (1, 'festival', 'Win a trip to Coachella', 10),
       (2, 'festival', 'Lollapalooza Beckons!', 5),
       (3, 'festival', 'Fye Fest 2.0 is ON!', 1),

       (5, 'streaming', 'Try Snowpipe Streaming today!', 20),
       (6, 'streaming', 'Give Dynamic Tables a spin at Snowflake', 20),
       (7, 'streaming', 'Use Streams + Tasks for your imperative CDC needs!', 20),
       (8, 'streaming', 'Have Snowpipe continuously ingest your data files', 20),

       (9, 'vegas', 'What happens in Vegas stays in Vegas', 1),
       (10, 'vegas', 'Attend the Snowflake Summit in Vegas!', 15),

       (11, 'lag', 'Dynamic Tables, now with LAG!', 8),
       (12, 'latency', 'Dynamic Tables, with latency for your use case!', 8),
       (13, 'database', 'Snowflake, the dAIta platform', 10),
       (14, 'pipe', 'Pipe Your Data into Snowflake using Snowpipe!', 10),
       (15, 'channel', 'Channel your inner data ninja with Snowpipe Streaming!', 10),
       (16, 'delay', 'Pipeline delay? Use Dynamic Tables today!!', 10),
       (17, 'aggregation', 'Dynamic Tables, now with aggregation support!', 10),
       (18, 'aggregations', 'Dynamic Tables, now with aggregations!', 10),
       (18, 'aggregated', 'Dynamic Tables, now with aggregations!', 10),
       (19, 'production', 'Snowpipe Streaming + DTs, prod ready!', 10),
       (20, 'help', 'Let Snowpipe Streaming + DTs help you!', 10),
       (21, 'help', 'Load your files with COPY!', 10),
       (21, 'files', 'Load your files with Snowpipe or COPY!', 10),
       (22, 'dynamic', 'Tables, now with more DYNAMIC!', 15),
       (23, 'tables', 'Dynamic, now with more TABLES!', 15),
       (24, 'flakes', 'Start using Snowflake today!', 15),
       (25, 'snowflake', 'Greatest cloud data platform EVER!', 15),
       (26, 'session', 'Go to Session WN210 to learn more!', 15),
       (27, 'sessions', 'Go to Session WN210 to learn more!', 15),
       (28, 'storage', 'Separate from Compute, the Snowflake way!', 15),
       (28, 'compute', 'Separate from Storage, the Snowflake way!', 15),
       (29, 'row', 'Snowpipe Streaming, now with rows!', 15),
       (29, 'rows', 'Snowpipe Streaming, now with rows!', 15),

       (150, 'swift', 'Please contact Tyler if you have TSwift tix', 4),
       (150, 'taylor', 'Please contact Tyler if you have TSwift tix', 4),
       (150, 'ticket', 'Please contact Tyler if you have TSwift tix', 4),

       (200, 'microphone', 'A pair of Neumann cardiods could help!', 7),
       (201, 'microphone', 'DPA mics save the day!', 6),
       (202, 'microphone', 'Having trouble recording? Use Schoeps MK4s!', 8),


       (100, 'rug', 'Take any rug you would like at the Big Lebowski''s', 5)
       """)
    con.cursor().execute(f"""
        create or replace dynamic table {WORD_COUNT_DT_NAME}
        LAG = '1 minute'
        WAREHOUSE={WAREHOUSE_NAME}
        AS
        SELECT t.user_id, t.word, count(*) as total
        FROM {TABLE_NAME} t
        where t.word not in (select word from {COMMON_WORDS_TABLE_NAME})
        GROUP BY 1, 2""")
    con.cursor().execute(f"alter dynamic table {WORD_COUNT_DT_NAME} refresh")
    con.cursor().execute(f"""
        create or replace dynamic table {ADS_WORD_SCORE_DT_NAME}
        LAG = '1 minute'
        WAREHOUSE={WAREHOUSE_NAME}
        AS
        select t.user_id as user_id, a.target_word, a.ad_id, a.target_ad_name, a.weight*t.total as score
        from {WORD_COUNT_DT_NAME} as t
        left join {AD_WORDS_TABLE_NAME} a
        on t.word = a.target_word
        where a.ad_id is not null
        order by score desc
        """)
    con.cursor().execute(f"alter dynamic table {ADS_WORD_SCORE_DT_NAME} refresh")


def remove_audio():
    """ Cleans up audio from a previous run."""
    files = glob.glob(f"{AUDIO_BASE_DIR}*")
    for f in files:
        os.remove(f)
    files = glob.glob(f"{AUDIO_RAW_DIR}*")
    for f in files:
        os.remove(f)


def print_to_info_line(screen, msg):
    """ Prints to the top "info" line with a message."""
    screen.clear()
    screen.addstr(0, 0, msg)
    screen.refresh()


def record_audio():
    global START_RECORDING
    counter = 0
    while True:
        if START_RECORDING:
            filename = AUDIO_RAW_DIR + \
                str(datetime.now(timezone.utc).timestamp() * 1000).split('.')[0] + \
                f'{counter}-speech.wav'
            destination_file = AUDIO_BASE_DIR + \
                str(datetime.now(timezone.utc).timestamp() * 1000).split('.')[0] + \
                f'{counter}-speech.wav'
            try:
                with wave.open(filename, 'wb') as wf:
                    p = pyaudio.PyAudio()

                    info = p.get_host_api_info_by_index(0)
                    numdevices = info.get('deviceCount')
                    device_to_use = None
                    for i in range(0, numdevices):
                        device = p.get_device_info_by_host_api_device_index(0, i)
                        if (device.get('maxInputChannels') > 0 and device.get('name') == 'MacBook Pro Microphone'):
                            device_to_use=i

                    wf.setnchannels(CHANNELS)
                    wf.setsampwidth(p.get_sample_size(FORMAT))
                    wf.setframerate(RATE)
                    stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, input_device_index=device_to_use, input=True)
                    for _ in range(0, RATE // CHUNK * RECORD_SECONDS):
                        wf.writeframes(stream.read(CHUNK))
                    stream.close()
                    p.terminate()
                counter+=1
                shutil.move(filename, destination_file)
            except Exception as e:
                pass
        else:
            time.sleep(1)


def get_cursor():
    """ Returns a cursor used to connect to the target account."""
    return snowflake.connector.connect(
        user=os.getenv('SNOW_USER'),
        password=os.getenv('SNOW_PW'),
        account=os.getenv('SNOW_ACCOUNT'),
        role=os.getenv('SNOW_ROLE')
    )

def realtime_thread():
    """ Continually reads from the stream in a table."""
    con = get_cursor()
    con.cursor().execute(f"use warehouse {WAREHOUSE_NAME}")
    stream_win = curses.newwin(WIN_HEIGHT, int((WIN_WIDTH/2)-1), PANEL_START_Y+1, int(WIN_WIDTH/2+1))
    while True:
        con.cursor().execute(f"begin;")
        words = con.cursor().execute(f"""
            select word
            from {DATABASE_NAME}.{SCHEMA_NAME}.{STREAM_NAME}
            order by word desc
            limit 20;
        """).fetchall()
        i = 1
        stream_win.clear()
        for word in words:
            stream_win.addstr(i,0,f"{word[0]}", curses.color_pair(1))
            i+=1
        stream_win.refresh()
        con.cursor().execute(f"""
            insert into {DATABASE_NAME}.{SCHEMA_NAME}.{ADVANCE_TABLE_NAME}
            select word from {DATABASE_NAME}.{SCHEMA_NAME}.{STREAM_NAME}""")
        con.cursor().execute(f"commit;")
        time.sleep(1)

def monitor_thread():
    """ Monitors the state of the table""" 
    #height, width, begin_y, begin_x
    stream_win = curses.newwin(WIN_HEIGHT, int((WIN_WIDTH/2)-1), PANEL_START_Y+1, int(WIN_WIDTH/2+1))
    monitor_win = curses.newwin(WIN_HEIGHT, WIN_WIDTH-1, PANEL_START_Y+1, WIN_WIDTH+1)
    score_win = curses.newwin(WIN_HEIGHT, WIN_WIDTH+30, PANEL_START_Y+1, WIN_WIDTH*2+1)
    con = get_cursor()
    con.cursor().execute(f"use warehouse {WAREHOUSE_NAME}")
    while True:
        time.sleep(10)
        con.cursor().execute(f"alter dynamic table {DATABASE_NAME}.{SCHEMA_NAME}.{WORD_COUNT_DT_NAME} refresh")
        con.cursor().execute(f"alter dynamic table {DATABASE_NAME}.{SCHEMA_NAME}.{ADS_WORD_SCORE_DT_NAME} refresh")
        word_counts = con.cursor().execute(f"""
            select word, total
            from {DATABASE_NAME}.{SCHEMA_NAME}.{WORD_COUNT_DT_NAME}
            order by total desc
            limit 20;
        """).fetchall()
        scores = con.cursor().execute(f"""
            select target_word, ad_id, target_ad_name, score
            from {DATABASE_NAME}.{SCHEMA_NAME}.{ADS_WORD_SCORE_DT_NAME}
            order by score desc
            limit 20;
        """).fetchall()
        monitor_win.clear()
        score_win.clear()
        i = 1
        for word in word_counts:
            monitor_win.addstr(i,0,f"{word[0]}:", curses.color_pair(1))
            monitor_win.addstr(f" : {word[1]}", curses.color_pair(2))
            i+=1
        i = 1
        for score in scores:
            score_win.addstr(i,0,f"{score[0]}", curses.color_pair(1))
            score_win.addstr(f" - ")
            score_win.addstr(f"{score[1]}", curses.color_pair(3))
            score_win.addstr(f" - ")
            score_win.addstr(f"{score[2]}", curses.color_pair(4))
            score_win.addstr(f" - ")
            score_win.addstr(f"{score[3]}", curses.color_pair(5))
            i+=1
        monitor_win.refresh()
        score_win.refresh()


def get_words():
    """ Generator to get words from audio input and returns a list of words.

    Returns: list of strings identified in the audio
    """
    last_seen_file = None
    fset = set()
    while True:
        files = glob.glob(f"{AUDIO_BASE_DIR}*.wav")
        for f in files:
            if last_seen_file is None:
                last_seen_file = f
            if f <= last_seen_file:
                continue
            try:
                last_seen_file = f
                result = MODEL.transcribe(f, fp16=False)
                res = result['text'].split(" ")
                yield res
            except Exception as e:
                yield []
            fset.add(f)

def init_curses():
    """ Initializes curses.

    Returns:
        screen object
    """
    screen = curses.initscr()
    curses.start_color()
    curses.init_pair(1, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_BLUE, curses.COLOR_BLACK)
    curses.curs_set(0)
    return screen


def init_headers(screen):
    """ Sets up headers and a border around key parts.
    """
    screen.clear()
    screen.addstr(0, 0, "Snowpipe Streaming + DTs Ad Recommendations Demo v1.0", curses.A_BOLD)
    screen.addstr(1, 0, "\"Leave the phone at home!\"")
    screen.addstr(PANEL_START_Y-1, 0, "Words Recognized:", curses.A_BOLD)
    screen.addstr(PANEL_START_Y-1, int(WIN_WIDTH/2), "New Words in Snowflake:", curses.A_BOLD)
    screen.addstr(PANEL_START_Y-1, WIN_WIDTH, "Word + Count in Snowflake:", curses.A_BOLD)
    screen.addstr(PANEL_START_Y-1, WIN_WIDTH*2, "Ad Matches in Snowflake (Word + Ad ID + Ad Tagline + Score):", curses.A_BOLD)
    try:
        screen.addstr(PANEL_START_Y, 0, "-"*(SCREEN_WIDTH+5))
        screen.addstr(SCREEN_HEIGHT-10, 0, "-"*(SCREEN_WIDTH+5))
        for i in range(PANEL_START_Y+1,WIN_HEIGHT+10):
            screen.addstr(i, 0, "|")
            screen.addstr(i, int(WIN_WIDTH/2), "|")
            screen.addstr(i, WIN_WIDTH, "|")
            screen.addstr(i, WIN_WIDTH*2, "|")
            screen.addstr(i, WIN_WIDTH*3+35, "|")
        screen.refresh()
    except Exception as e:
        # The screen isn't properly resized
        print_to_info_line(screen, "Please re-size the screen to fit the demo, exiting...")


def main():
    # Init curses
    screen = init_curses()

    # Clean any previous transcriptions
    remove_audio()

    # Initialize DB, WH
    print_to_info_line(screen, "Initializing Snowflake connection, creating warehouse + base tables...")
    con = get_cursor()
    con.cursor().execute(f"create warehouse if not exists {WAREHOUSE_NAME} warehouse_size=xsmall;")
    create_ads_table(con)

    # Start our monitoring thread
    print_to_info_line(screen, "Starting monitoring thread....")
    th = threading.Thread(target=monitor_thread)
    sh = threading.Thread(target=realtime_thread)
    th.start()
    sh.start()

    # Start recording audio. We do this prior to the subprocess due to
    # some quirk with opening the audio device input after the fact
    print_to_info_line(screen, "Opening microphone....")
    ah = threading.Thread(target=record_audio)
    ah.start()

    # Now let's pipe our audio to Snowflake!
    print_to_info_line(screen, "Starting Snowpipe Streaming Client....")
    command = ['java', '-jar', 'streaming-ingest-file-gen/target/FileGenerator.jar']
    log = open('plog.txt', "w", 1)  # line-buffered
    p = Popen(command, stdin=PIPE, stdout=log, stderr=log)
    time.sleep(10)
    valid_words = 0
    global START_RECORDING
    START_RECORDING = True
    print_to_info_line(screen, "Starting to Record")
    init_headers(screen)

    word_win = curses.newwin(WIN_HEIGHT, int(WIN_WIDTH/2-1), PANEL_START_Y+1, 1)
    word_counter_win = curses.newwin(1,150, PANEL_START_Y-2, 0)
    index = 1
    while True:
        try:
            for word in get_words():
                for w in word:
                    if w is None or w in WORDS_TO_IGNORE:
                        continue
                    w = w.lower()
                    for char in CHARS_TO_REPLACE:
                        w = w.replace(char, '')
                    if w.endswith('.') or w.endswith(',') or w.endswith('!'):
                        w = w[:-1]
                    valid_words+=1
                    word_win.addstr(index, 0, f"{w}", curses.color_pair(1))
                    index+=1
                    p.stdin.write(f"{w}\n".encode("utf-8"))
                    p.stdin.flush()
                    word_win.refresh()
                    if index == WIN_HEIGHT:
                        index = 1
                        word_win.clear()
                    word_counter_win.clear()
                    word_counter_win.addstr(0, 0, f"Word Count: {valid_words}")
                    word_counter_win.refresh()
        except Exception as e:
            p = Popen(command, stdin=PIPE, stdout=DEVNULL, stderr=DEVNULL)
            time.sleep(10)
            


if __name__ == '__main__':
    curses.wrapper(main())
