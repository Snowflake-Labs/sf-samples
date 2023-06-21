# sis-local

Emulate SiS functionality in a local environment. This may not work perfectly, but it should catch many common errors
and provide a much smoother experience doing local development and porting to SiS!

- Throw warnings for commands that don't work in SiS
- (optional) Handles local Snowpark session setup

**If you find some missing compatibility issue, please file an issue and tag `@sfc-gh-jcarroll`**

## Usage

### Installation

Just like Snowpark, **it requires python 3.8.**

```shell
pip install "streamlit_in_snowflake @ git+https://github.com/Snowflake-Labs/sf-samples.git#subdirectory=samples/streamlit-in-snowflake/sis-local"
```

In general, we recommend managing your environment with Conda,
as it will transition easily to the SiS environment. However, this package is not yet available in the [Snowflake Anaconda channel](https://repo.anaconda.com/pkgs/snowflake/),
so it may be useful to install separately once your conda environment is setup.

See the example directory for a minimal conda setup. Read more in the
[SiS docs](https://docs.snowflake.com/en/LIMITEDACCESS/streamlit-in-snowflake#installing-packages-manually).

### Magic code block for the top of your app

```python
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSessionException


def running_in_sis() -> bool:
    import snowflake.connector.connection
    import inspect
    # snowflake.connector.connection.SnowflakeConnection does not exist inside a Stored Proc or Streamlit.
    # It is only part of the external package. So this returns true only in SiS.
    return 0 == len([x for x in inspect.getmembers(snowflake.connector.connection) if x[0] == 'SnowflakeConnection'])


if running_in_sis():
    import streamlit as st
    session = get_active_session()
else:
    import streamlit_in_snowflake as st
    # This part is optional, and requires secrets.toml setup
    from streamlit_in_snowflake.local_session import get_local_session
    session = get_local_session()

# ...
```

### Configuring secrets.toml for `LocalSnowparkConnection`

LocalSnowparkConnection reads from `.streamlit/secrets.toml`. By default it looks for
a Snowflake connection config under the `[connections.snowflake]` heading. You can
customize this by setting `connection_name=` in the init.

- **TODO:** Add support for reading from snowsql config file.

```toml
# Example config for .streamlit/secrets.toml
[connections.snowflake]
user = "username"
warehouse = "MYWAREHOUSE"
role = "MYROLE"
account = "MYACCOUNT"
authenticator = "externalbrowser"
connection_timeout = "600"
```
