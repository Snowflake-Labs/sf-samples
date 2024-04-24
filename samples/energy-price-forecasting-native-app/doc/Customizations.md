# Customizations

## Libraries
While the Codespace docker image is burnt with some basic libraries; it is highly possible that your project would
require additional libraries. May be pytorch, tensorflow etc.. In these cases, you might be wondering how to go about adding these required libraries.

Not to worry, based on [Doc:Creating an environment file manually](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually), we have the file [conda_pyspark_environment.yml](../.devcontainer/conda_pyspark_environment.yml) defined.

You can add the library into the list. Be aware the library will be sourced from either of these channels as defined:
- Snowflake Anaconda channel
- Conda defaults channel

