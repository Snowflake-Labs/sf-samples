# Demo Cortex Analyst Native App 
This repository contains assets and code for the Medium Blog "Enable Natural Language Querying of Your Snowflake Marketplace Data with Native Apps & Cortex Analyst" by Rachel Blum & Rich Murnane, Snowflake Solution Innovation Team.

Pre-reqs for this application include:
- Clone or download this Snowflake-Labs github repository
- Verify [Cortex Analyst availability](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst#region-availability) in your Snowflake region (currently in PuPr)
- A Snowflake role with elevated privileges (example given in provider.sql script), or the ACCOUNTADMIN role to your Snowflake account
- Execution of [Step 1](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/tutorials/tutorial-1#step-1-setup) and [Step 2](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/tutorials/tutorial-1#step-2-load-the-data-into-snowflake) of the Snowflake Cortex Analyst Tutorial documented here. This will provide the sample data and semantic .yaml file required for the application.


## Apache 2.0 license
```
Copyright 2024 Rich Murnane Snowflake

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```