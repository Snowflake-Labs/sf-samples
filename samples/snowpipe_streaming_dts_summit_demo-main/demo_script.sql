-- Creates the role used for the Demo
use role accountadmin;
create or replace role tjones_summit_rl;
grant create database on account to role tjones_summit_rl;
grant create warehouse on account to role tjones_summit_rl;
grant role tjones_summit_rl to user tjones;
use role tjones_summit_rl;

-- Create a warehouse to use for the demo
create or replace warehouse tjones_wh 
use warehouse tjones_wh;
select 'Hello Summit!';


-- Create a database/schema to use for the demo.
create or replace database tjonesdb;
use database tjonesdb;
create or replace schema summit_sch;
use schema summit_sch;


-- Create our staging table that will get populated by Snowpipe Streaming
create or replace table landing_words_t(user_id int, word varchar) change_tracking=true;
select user_id, word from landing_words_t;


create or replace table common_words_t (word varchar);
insert into common_words_t
values ('you'), ('what'), ('uh'), ('um'), ('me'),
       ('the'), ('this'), ('thing'), ('that'), ('be'),
       ('a'), ('of'), ('in'), ('is'), ('it'), ('have'),
       ('and'), ('it''s'), ('there'), ('there''s'), ('too'),
       ('i'), ('if'), ('to'), ('me'), ('was'), ('for'), ('get'), ('my'),
       ('oh'), ('something'), ('that''s'), ('do'), ('with'), ('we'),
       ('can'), ('do'), ('we'), ('with'), ('from'), ('so'), ('all'),
       ('are'), ('our'), ('should'), ('would'), ('like'), ('yeah'),
       ('mhm'), ('were'), ('we''ll'), ('no')
;

-- Create our table with Target Ad Words and associated weights
create or replace table ads_words_t (ad_id int, target_word varchar, target_ad_name varchar, weight int);
insert into ads_words_t(ad_id, target_word, target_ad_name, weight) values
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
       (19, 'production', 'Snowpipe Streaming + DTs, prod ready!', 10),

       (200, 'microphone', 'A pair of Neumann cardiods could help!', 7),
       (201, 'microphone', 'DPA mics save the day!', 6),
       (202, 'microphone', 'Having trouble hearing? Use Schoeps MK4s!', 8),
       ;

-- Create a stream on that table
create or replace stream landing_words_stream on table landing_words_t;
select user_id, word from landing_words_stream;


-- Create our dynamic table that aggregates word counts from the staging table
-- filtering out common words
create or replace dynamic table word_aggregation_dt
lag = '1 minute'
warehouse = tjones_wh
as select t.user_id, t.word, count(*) as total
from landing_words_t as t
where t.word not in (select * from common_words_table_name)
group by 1,2
alter dynamic table word_aggregation_dt refresh;
select user_id, word, total from word_aggregation_dt;


-- Create our dynamic table that recommends what ads be shown based on word counts
create or replace dynamic table ad_results_t
lag = '1 minute'
warehouse = tjones_wh
as
select t.user_id as user_id, a.target_word, a.ad_id, a.target_ad_name, a.weight*t.total as score
from word_aggregation_dt as t
left join ads_words_t a
on t.word = a.target_word
where a.ad_id is not null
order by score desc;
alter dynamic table ad_results_t refresh;
select user_id, target_word, target_ad_name, score from ad_results_t;

