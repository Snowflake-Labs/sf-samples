# Changelog

## Future ToDo:
 - COSTCENTER table exists in each of the Streamlit app schemas, should we centralize it?

## [1.3.1] - 2025-10-10
 - Deployment code and security script fixes for Streamlit apps

## [1.3] - 2025-07-18
 - Map to warehouse_id instead of name in tag_references join in case of name changes and performance.
 - Bug fixes for Streamlit apps not working.
 - Bug fixes for views
 - Readme updates

## [1.2] - 2025-05-20
  - Major update to how cost per query is calculated requiring full redo of SF_CREDITS_BY_QUERY table data and updated schema.
    - Added QUERY_LOAD_PERCENT to tables and use it in calculation to potentially decrement the queries cost based on this percentage.
    e.g. if the value is 25% then the adjustment of warehouse weight for that query is multiplied by 25%
  - Updated linear forecast view to handle multi-year contract periods.

## [1.1.3] - 2025-04-11

 - Bug fixes in:
   - Increase size of object names in SF_CREDITS_BY_QUERY* tables.
   - Additional reference fixes for Streamlit installation.

## [1.1.2] - 2025-04-08

- Additional Cortex credit views to cover remaining Cortex costs
- Example tag script major updates
- Bug fixes in streamlit config and python file paths


## [1.1.1] - 2025-04-03

- Improvements to cost per query filters

## [1.1.0] - 2025-04-02

- Bug fixes in: 
    - sf_credit_tables lookup table population
    - 00/01 scripts
    - snowflake.yml
    - tagging module scripts example fixes
- Added alter_user_set_wh.sql for testing purposes mostly, but still useful.
- Readme updates to improve readability and add more PC command updates

## [1.0.1] - 2025-03-24

- Added CORTEX_FUNCTION_CC_TOKEN_CREDITS_DAY view

## [1.0.0] - 2025-03-19

- First release of FinOps Framework code base.
- Includes Streamlit Code deployment for visibility app in this update.

## [0.9.1] - 2025-01-21

### Updates

- Update instructions and migrate to using Snow CLI as a deployment mechanism.

## [0.9.0] - 2025-01-08

### Released

- Initial release of the project. This is first version of the project packaged as a ready to use that encapsulated 2 years worth of effort from Chuck Lathrope, Sr. Solution Architect, Snowflake.
