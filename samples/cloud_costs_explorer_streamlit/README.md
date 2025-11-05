# Cloud Costs Explorer

This project is a Streamlit application designed to provide insights into cloud costs across various resources. The application is structured to offer detailed summaries and forecasts for compute, storage, and network costs.

## Directory Structure

- **environment.yml**: Contains the dependencies required for the project.
- **snowflake.yml**: Configuration for Snowflake integration.
- **streamlit_app.py**: Main entry point for the Streamlit application.
- **pages/**: Directory containing the individual page scripts for the application.
- **.streamlit/**: Directory for Streamlit configuration files.
- **pages.toml**: Configuration for page settings.
- **__init__.py**: Marks the directory as a Python package.
- **.envrc**: Environment configuration file.

## Dependencies

The project requires the following dependencies, as specified in `environment.yml`:

- **Python 3.11.9**: The specified version of Python.
- **Streamlit 1.39.0**: The version of Streamlit used for building the app.
- **snowflake-snowpark-python**: A library for working with Snowflake's Snowpark.
- **sqlparse**: A library for parsing SQL queries.
- **plotly**: A library for creating interactive plots.



## Dummy Data

⚠️ **Important**: The current data displayed in this application is generated for demo purposes only. This includes all charts, tables, and metrics shown in the dashboard. 

**To use this template with your actual data:**
1. Replace the dummy data sources with connections to your real data
2. Update the SQL queries in the pages to reference your actual tables
3. Modify the data processing logic to match your data schema
4. Test thoroughly with your data to ensure accuracy

## Usage

To run the application, ensure all dependencies are installed and execute the `streamlit_app.py` using Streamlit:

```bash
streamlit run streamlit_app.py
```

This will start the Streamlit server and open the application in your default web browser.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.

## About the Template

This application serves as a template for analyzing cloud costs. It is designed with dummy generated data to demonstrate the functionality and layout of a cloud cost analysis tool. Users can customize the template by integrating real data sources and modifying the existing pages to fit specific requirements.

## Configuration

If you are an external customer using this Streamlit app, you may need to update the `env` section within `snowflake.yml` to match your specific environment settings. This includes:

- **role**: Update to the Snowflake role that your organization uses.
- **name**: Customize the name of the app environment to fit your organization's naming conventions.
- **app_warehouse** and **query_warehouse**: Set these to warehouses that you have access to and are appropriate for your usage.
- **share_with_roles**: Update to include roles that are relevant to your organization.
- **title**: Customize the title of the app to reflect your branding or specific use case.

These updates ensure that the app is configured correctly for your Snowflake environment and access permissions.

### Streamlit Configuration

External customers will likely need to update the files under the `.streamlit` directory to match their specific environment settings:

1. **`secrets.toml`**:
   - **account**: Update to your Snowflake account name.
   - **user**: Change to the appropriate user for your environment.
   - **role**: Update to the Snowflake role that you use.
   - **database**: Set to the database you will be using.
   - **warehouse**: Update to the warehouse that you have access to.
   - **schema**: Set to the schema you will be using.
   - **host**: Update to your Snowflake host URL.

2. **`config.toml`**:
   - This file contains theme settings, which are generally not specific to the environment but can be customized for visual preferences.

These updates ensure that the app is configured correctly for your Snowflake environment and access permissions.
