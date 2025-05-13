# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time
from streamlit import session_state as ss
from snowflake.snowpark.functions import when_matched, when_not_matched
import yaml

st.set_page_config(layout="wide")

# Write directly to the app
st.title(":blue[FINOPS TAGGING]")

with open('finops_tag/snowflake.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader).get('env')

fqn_tag = '.'.join([config.get('finops_sis_db'),config.get('finops_sis_tag_sc'), config.get('finops_tag_name')])

username = st.experimental_user.user_name
email = st.experimental_user.email
welcome_message = f"Welcome:  {username}"
st.write(welcome_message)
st.write(fqn_tag)

# Get the current credentials
session = get_active_session()

def get_dataset(tbname):
    # load messages df
    df = session.table(tbname).to_pandas()
    # rebuild the index for the datafram
    df.reset_index(inplace = True, drop = True)
    return df

# Process the columns for the updated record for the UPDATE statement
def process_cols(columns):
    i = 0
    stmt = ""
    for c in columns:
        if i == 0:
            stmt = "UPDATE " + tabname + " SET " + c + " = '" + columns[c] + "'"
            i = 5
        else:
            stmt = stmt + ", " + c + " = '" + columns[c] + "'"
    return stmt

# Get the columns and column values and build out the UPDATE statement
def select_cols (df, idx):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

# Get the columns and column values and build out the INSERT statement
def insert_cols(cols):
    first = True
    stmt = ""
    vals = ""
    for col in cols:
        if first:
            stmt = "INSERT INTO " + tabname + " ( " + col
            vals = " VALUES ('" + str(cols[col]) + "'"
            first = False
        else:
            stmt = stmt + ", " + col
            vals = vals + ", '" + str(cols[col]) + "'"
    return stmt + ") " + vals + ")"

# Get the columns / values for the DELETE statement
def delete_cols(idx, df):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = "DELETE FROM " + tabname + " WHERE " + col + " = '" + str(df.iloc[idx][col]) + "'"
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None':
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

if "de_key" not in st.session_state:
    st.session_state["de_key"] = 1

def version_key():
    st.session_state["de_key"] += 1

def check_change(check_all,  multiselect_options, list_items ):
    if st.session_state[check_all]:
        st.session_state[multiselect_options]  = list_items
    else:
        st.session_state[multiselect_options] = []
    return

def multi_change(check_all, multiselect_options, list_items):
    if len(st.session_state[multiselect_options] ) == len(list_items):
        st.session_state[check_all] = True
    else:
        st.session_state[check_all] = False
    return

@st.cache_data
def get_data(id: int):
    df = session.sql('select * FROM OBJECT_COSTCENTER_MAPPING').to_pandas()
    return df

tab1, tab2, tab3  = st.tabs(["Cost Center","Object Cost Center Mapping","Tagging"])

# Tagging Module
with tab1:
    tabname = "COSTCENTER"

# Get the Table as a dataframe
#
    dataset = get_dataset(tabname)
    st.caption("Edit the Cost Center below")
    ss.edited = st.data_editor(dataset, use_container_width=True, num_rows="dynamic", key='ed',disabled='ID',column_config={'ID': st.column_config.NumberColumn(help='ID will be auto populated')})
    submit_button = st.button("Save")

    if submit_button:
        idx = 0

        try:

            # Process updated row(s)
            for rec in ss.ed["edited_rows"]:
                idx = int(rec)
                updt = process_cols(ss.ed["edited_rows"][rec])
                where = select_cols(dataset, idx)

                update_stmt = updt + " " + where

                result = session.sql(update_stmt).collect()
                st.write(result)

            # Process newly inserted row(s)
            for irec in ss.ed["added_rows"]:
                insert_stmt = insert_cols(irec)

                result = session.sql(insert_stmt).collect()
                st.write(result)

            # Process the deleted row(s)
            for rec in ss.ed["deleted_rows"]:
                idx = int(rec)
                delete_stmt = delete_cols(idx, dataset)
                result = session.sql(delete_stmt).collect()
                st.write(result)


            time.sleep(2)
            st.write("Record Saved Successfully")
            st.rerun()

        except Exception as e:
            st.write(e)
            st.warning("Error updating table")
            time.sleep(2)
            st.rerun()

# Editing Module

with tab2:
    st.header(":blue[Edit Object Cost Center Mapping]")
    #st.write('Update Mapping Table')
    session = get_active_session()


        #load view with current data
    df = get_data(1)
    col1,col2 = st.columns([1,1])
    col2,col5 = st.columns([1,1])
    savecol,rollbackcol,applytag = st.columns(3)

    with col1:
            list_sfuserids = df["OBJECT_TYPE"].unique().tolist()

            if "all_option_sfuserid" not in st.session_state:
                st.session_state.all_option_sfuserid = True
                st.session_state.selected_options_storeid = list_sfuserids

            selected_storeids = st.multiselect(
                "**Select Object Type to edit**"
                ,list_sfuserids
                ,key="selected_options_storeid"
                ,on_change= multi_change
                ,args=("all_option_sfuserid","selected_options_storeid", list_sfuserids )
                ,help = 'Select the objecttype from button'
                #,default = list(['All'])
                ,max_selections=30
            )

            all_storeids = st.checkbox(
                "Select all"
                ,key='all_option_sfuserid'
                ,on_change= check_change
                ,args=("all_option_sfuserid","selected_options_storeid", list_sfuserids )
            )

    select_df = session.sql("select costcenter_name from costcenter").to_pandas()
    filter_data=select_df
    filter_data1=filter_data.values.tolist()

    df = get_data(1)

    with col2:
        edited_df = st.data_editor(df.loc[ ( df["OBJECT_TYPE"].isin(selected_storeids) )],key=f"""dyn_edit{st.session_state["de_key"]}""",column_config={"COSTCENTER": st.column_config.SelectboxColumn("COSTCENTER_NAME", help="Cost Center Name", width="medium", options=filter_data1, required=True)},disabled=('OBJECT_TYPE','OBJECT_NAME'),use_container_width=True,)

    with savecol:
        update = st.button("Save ")
    with rollbackcol:
        rollback = st.button("Rollback Edit",on_click=version_key)
    with applytag:
        applytag = st.button("ApplyTag",help="Apply Tag")

    if update:
        comparison = pd.merge(df,edited_df,how='outer',indicator=True, suffixes=("_Original","_Edited"), copy=True,)
        temp_insert = comparison.loc[comparison["_merge"] == "right_only"]

        if (temp_insert.shape[0] > 0):
            final_df = session.create_dataframe(temp_insert)
            target = session.table("OBJECT_COSTCENTER_MAPPING")
            source = final_df
            target.merge(source, (target["OBJECT_NAME"] == source["OBJECT_NAME"]),
                    [
                    when_matched().update({
                    "OBJECT_TYPE": source["OBJECT_TYPE"],
                    "COSTCENTER": source["COSTCENTER"],

                    }),

                    # when_not_matched().insert({
                    #     "OBJECT_NAME" : source["OBJECT_NAME"],
                    #     "OBJECT_TYPE": source["OBJECT_TYPE"],
                    #     "COSTCENTER": source["COSTCENTER"],
                    #     })
                     ]
                        )
            st.write("Data Saved Successfully")
            st.cache_data.clear()
            st.rerun()
        else:
            st.write("No Changes are made")

    if rollback:
        st.button("Reset", on_click=version_key)
        st.rerun()
    # For the case when the tag name has changed using the stream on the table.
    if applytag:
        session.sql(f"create or replace transient table tagsql as select 'alter '||object_type||' \"'||object_name||'\" SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql  from st_object_costcenter_mapping where metadata$action='INSERT' and metadata$isupdate='TRUE'").collect()
        st.write("Started Applying Tag")
        session.sql("call exec_sql('tagsql')").collect()
        st.write("Successful Applied Tag")

with tab3:
    st.write("Tag Administration Module")
    object = st.radio(
        "***Select Object to Tag***",
        ["Database", "Schema", "Role","User","Warehouse"],
        index=None,
    )

    st.write("You selected:", object)


    generatecol, applycol, listcol = st.columns(3)

    with generatecol:
            generate = st.button('Generate Tag Script')

    with applycol:
            apply = st.button('Apply Tag')

    with listcol:
            list = st.button('List Tag',help='Delayed Data Refresh,Since using account usage view')


    # Generate Tag Script
    if generate:
      if (object == "User"):
        users = session.sql(f"create or replace transient table usersql as select 'alter '||object_type||' \"'||object_name||'\" SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql from object_costcenter_mapping where object_type='USER'").collect()
        userdata = session.sql("select * from usersql").collect()
        st.write(userdata)
      elif (object == "Role"):
        roles = session.sql(f"create or replace transient table rolesql as select 'alter '||object_type||' \"'||object_name||'\" SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql from object_costcenter_mapping where object_type='ROLE'").collect()
        roledata = session.sql("select sql from rolesql").collect()
        st.write(roledata)
      elif (object == "Database"):
        #session.sql("show databases").collect()
        dbs = session.sql(f"create or replace transient table databasesql as select 'alter '||object_type||' \"'||object_name||'\" SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql from object_costcenter_mapping where object_type='DATABASE' ").collect()
        databasedata = session.sql("select sql from databasesql").collect()
        st.write(databasedata)
      elif (object == "Schema"):
        #session.sql("CREATE OR REPLACE TRANSIENT TABLE ROLE_COST_CENTER AS SELECT OBJECT_NAME AS ROLE_NAME, TAG_VALUE AS COST_CENTER FROM TABLE(SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES_WITH_LINEAGE('UTIL_DB.ADMIN_TOOLS.COST_CENTER')) WHERE TRUE AND LEVEL = 'ROLE' AND TAG_NAME = 'COST_CENTER'").collect()
        schemas = session.sql(f"CREATE OR REPLACE TRANSIENT TABLE SCHEMASQL AS select 'alter '||object_type||' '||object_name||' SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql from object_costcenter_mapping where object_type='SCHEMA' ").collect()
        schemadata = session.sql("select sql from schemasql").collect()
        st.write(schemadata)
      elif (object == "Warehouse"):
        #session.sql("show warehouses").collect()
        session.sql(f"create or replace transient table warehousesql as select 'alter '||object_type||' '||object_name||' SET TAG {fqn_tag} = '''||coalesce(costcenter,'UNKNOWN')||'''' as sql from object_costcenter_mapping where object_type='WAREHOUSE' ").collect()
        warehousedata = session.sql("select sql from warehousesql").collect()
        st.write(warehousedata)
      else:
          st.write("Select Object Type to Generate Tag Script")


    # Apply Tag
    if apply:
        if (object == "User"):
            tab_exists = session.sql(" select count(*) as cnt from information_schema.tables where table_name='USERSQL'").to_pandas()
            u_tab = (tab_exists.iloc[0]["CNT"])

            if (u_tab == 1):
                st.write("Started Applying Tag for User")
                session.sql("call exec_sql('usersql')").collect()
                st.write("Applied Tag for User")
            else:
                st.write("Generate Tag script first and then Apply Tag")
        elif (object == "Role"):
            r_tab_exists = session.sql(" select count(*) as cnt  from information_schema.tables where table_name='ROLESQL'").to_pandas()
            r_tab = (r_tab_exists.iloc[0]["CNT"])
            if (r_tab == 1):
                st.write("Started Applying Tag for Role")
                session.sql("call exec_sql('rolesql')").collect()
                st.write("Applied Tag for Role")
            else:
                st.write("Generate Tag script first and then Apply Tag")
        elif (object == "Database"):
            d_tab_exists = session.sql(" select count(*) as cnt  from information_schema.tables where table_name='DATABASESQL'").to_pandas()
            d_tab = (d_tab_exists.iloc[0]["CNT"])
            if (d_tab == 1):
                st.write("Started Applying Tag for Database")
                session.sql("call exec_sql('databasesql')").collect()
                st.write("Applied Tag for Database")
            else:
                st.write("Generate Tag script first and then Apply Tag")
        elif (object == "Schema"):
            s_tab_exists = session.sql(" select count(*) as cnt from information_schema.tables where table_name='SCHEMASQL'").to_pandas()
            s_tab = (s_tab_exists.iloc[0]["CNT"])
            if (s_tab == 1):
                st.write("Started Applying Tag for Schema")
                session.sql("call exec_sql('schemasql')").collect()
                st.write("Applied Tag for Schema")
            else:
                st.write("Generate Tag script first and then Apply Tag")

        elif (object == "Warehouse"):
            w_tab_exists = session.sql(" select count(*) as cnt  from information_schema.tables where table_name='WAREHOUSESQL'").to_pandas()
            w_tab = (w_tab_exists.iloc[0]["CNT"])
            if (w_tab == 1):
                st.write("Started Applying Tag for Warehouse")
                session.sql("call exec_sql('warehousesql')").collect()
                st.write("Applied Tag for Warehouse")
            else:
                st.write("Generate Tag script first and then Apply Tag")

        else:
          st.write("Select Object Type to Apply Tag")

    # List Tag
    if list:
        if (object == "User"):
            st.write("Listing Tags for Users:")
            list_data = session.sql("Select DOMAIN,OBJECT_NAME,TAG_NAME,TAG_VALUE from snowflake.account_usage.tag_references where domain = 'USER';").collect();
            st.write(list_data)
        elif (object == "Role"):
            st.write("Listing Tags for Role:")
            list_data = session.sql("Select DOMAIN,OBJECT_NAME,TAG_NAME,TAG_VALUE from snowflake.account_usage.tag_references where domain = 'ROLE';").collect();
            st.write(list_data)
        elif (object == "Database"):
            st.write("Listing Tags for Database:")
            list_data = session.sql("Select DOMAIN,OBJECT_NAME,TAG_NAME,TAG_VALUE from snowflake.account_usage.tag_references where domain = 'DATABASE';").collect();
            st.write(list_data)
        elif (object == "Schema"):
            st.write("Listing Tags for Schema:")
            list_data = session.sql("Select DOMAIN,OBJECT_NAME,TAG_NAME,TAG_VALUE from snowflake.account_usage.tag_references where domain = 'SCHEMA';").collect();
            st.write(list_data)
        elif (object == "Warehouse"):
            st.write("Listing Tags for Warehouse:")
            list_data = session.sql("Select DOMAIN,OBJECT_NAME,TAG_NAME,TAG_VALUE from snowflake.account_usage.tag_references where domain = 'WAREHOUSE';").collect();
            st.write(list_data)
        else:
          st.write("Select Object Type to list Tag")
