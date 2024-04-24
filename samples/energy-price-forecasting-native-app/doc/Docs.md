
- [Running CodeSpace](Running_CodeSpace.md)
- [Customizations](Customizations.md)

## How do i make a project "Codespaced"?
Use this project as the base code structure for the your project. What this means is that as a clean state, copy the content of this repo into your project.

Add new libraries, if needed based on [Doc: Customizations](Customizations.md)

For convienience, dont remove the [Doc: Running CodeSpace](Running_CodeSpace.md) and the "images" folder. But you can remove other documentations like [Doc: Customizations](Customizations.md).

Maintain the same project structure and ensure the artifacts (ex: python scripts) are stored in the appropriate folders. 

Try to use parameterization/variable substitution in your scripts, as much as possible.

### Developing Solutions Locally
Having your project 'codespace' enabled, helps in following an opinionated standard and also the community to test/demo the app using GitHub codespace environment.As much as GitHub codespace provides the ability to run, it is very costly if you are using codespace during development cycle. 

Hence have the regular tendency of developing your solutions locally using VScode, Snowsql and other normal development tools. Follow the details in the below section

### How to setup and run the demo (Locally)
In case if you do not have access to GitHub codespace environment, follow these steps to run the demo.

**NOTE:** It is assumed that you have following skils:
    - Hands on python, SQL, Snowflake developer
    - You know howto install and connect to Snowflake 
    - You are using or have used Visual Studio Code

#### Installation tools:
Install the following :

- [Anaconda](https://www.anaconda.com/)
- [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql-install-config.html) 
- [Visual Code](https://code.visualstudio.com/)

#### Setup project:
- Download this project
- Import the project into Visual Code
- Open terminal view (inside visual code) and goto project home dir.
- Run the following command to create a conda env named 'pysnowpark' and also the set of base libraries like jupyter, snowpark, streamlit etc..
```sh
    conda env update --file .devcontainer/conda_pyspark_environment.yml
```

    If you already have a env named 'pysnowpark' use this command:
```sh
    conda env update -n pysnowpark --file .devcontainer/conda_pyspark_environment.yml
```

- Create a local secrets file: sflk_connection.json based of the template file: [snowflake_connection.json.template](../snowflake_connection.template.json)
- Update configurations and app configurations specific to your solution under `APP_DB` section of [config.ini](../config.ini)
- validate if stuffs are running:
  - run streamlit app: ./bin/run_app.sh
  - run snowsql: ./bin/connect_snowsql.sh

