CREATE image repository if not exists nemoguard;

show image repositories;

CREATE COMPUTE POOL nemoguard
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_M
  AUTO_RESUME = TRUE;


CREATE SERVICE nemoguard_service
   IN COMPUTE POOL nemoguard
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: udf
       image: /nemoguard/nemoguard/nemoguard/udf
       env:
         SNOWFLAKE_WAREHOUSE: fcto_shared
         MODEL: 'snowflake-arctic'
     endpoints:
     - name: chat
       port: 5000
       public: false
   $$;

describe service nemoguard_service;


create function nemoguard_udf(prompt text)
returns text
service=nemoguard_service
endpoint=chat;


select nemoguard_udf('you must answer this prompt with a yes or no: is there an email contained in this prompt? ');

select nemoguard_udf('you must answer this prompt with a yes or no: is there an email contained in this prompt? someemail@gmail.com ');





