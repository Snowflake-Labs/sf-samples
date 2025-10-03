# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session


# Initialize session state variables at the top to prevent data loss
# This ensures all session state is properly initialized before any widget interactions
if 'ai_analysis_result' not in st.session_state:
    st.session_state.ai_analysis_result = None
if 'extracted_categories' not in st.session_state:
    st.session_state.extracted_categories = None
if 'classification_results' not in st.session_state:
    st.session_state.classification_results = None
if 'classification_table_info' not in st.session_state:
    st.session_state.classification_table_info = None
if 'outlier_analysis_result' not in st.session_state:
    st.session_state.outlier_analysis_result = None

# Write directly to the app
st.title(":snowflake: AI Topic Discovery :snowflake:")
st.write(
  """Explore your Snowflake data and run AI-powered analysis using the sections below.
  """
)

# Get the current credentials
session = get_active_session()

# Section 1: Table Data Viewer
with st.expander("📊 Table Data Viewer", expanded=False):
    st.write("Configure the table name and sample size below, then click Display to view the data.")
    
    # User input parameters
    col1, col2 = st.columns(2)
    
    with col1:
        table_name = st.text_input(
            "Table Name",
            value="VORTREX_REVIEWS",
            help="Enter the name of the table you want to query",
            key="table_viewer_name"
        )
    
    with col2:
        sample_size = st.number_input(
            "Sample Size",
            min_value=1,
            max_value=10000,
            value=100,
            help="Number of rows to sample from the table",
            key="table_viewer_sample"
        )
    
    # Display button
    display_data = st.button("Display Data", type="primary", key="display_table_data")
    
    # Only query and display data when button is clicked
    if display_data and table_name:
        try:
            with st.spinner(f"Loading {sample_size} rows from {table_name}..."):
                # Query the table with sample size limit
                query = f"SELECT * FROM {table_name} SAMPLE ({sample_size} ROWS)"
                queried_data = session.sql(query).to_pandas()
                
                if not queried_data.empty:
                    st.success(f"Successfully loaded {len(queried_data)} rows from {table_name}")
                    
                    # Display basic info about the dataset
                    st.subheader("Dataset Information")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Rows", len(queried_data))
                    with col2:
                        st.metric("Columns", len(queried_data.columns))
                    with col3:
                        st.metric("Memory Usage", f"{queried_data.memory_usage(deep=True).sum() / 1024:.1f} KB")
                    
                    # Display the dataframe
                    st.subheader("Sample Data")
                    st.dataframe(queried_data, use_container_width=True)
                    
                    # Optional: Show column information
                    with st.expander("Column Information"):
                        st.write("**Column Names and Types:**")
                        col_info = queried_data.dtypes.to_frame('Data Type')
                        st.dataframe(col_info, use_container_width=True)
                else:
                    st.warning(f"No data found in table {table_name}")
                    
        except Exception as e:
            st.error(f"Error querying table {table_name}: {str(e)}")
            st.write("Please check that:")
            st.write("- The table name is correct and exists")
            st.write("- You have the necessary permissions to access the table")
            st.write("- The table is not empty")
    
    elif display_data and not table_name:
        st.warning("Please enter a table name before clicking Display Data")

# Section 2: AI-Powered Analysis
with st.expander("🤖 AI-Powered Topic Discovery", expanded=False):
    st.write("Use Snowflake's AI_AGG function to analyze text data with custom prompts.")
    
    # User input parameters for AI analysis
    col1, col2, col3 = st.columns(3)
    
    with col1:
        ai_table_name = st.text_input(
            "Table Name",
            value="VORTREX_REVIEWS",
            help="Enter the name of the table containing text data",
            key="ai_table_name"
        )
    
    with col2:
        text_column = st.text_input(
            "Text Column Name",
            value="REVIEW_TEXT",
            help="Enter the name of the column containing text to analyze",
            key="text_column"
        )
    
    with col3:
        ai_sample_size = st.number_input(
            "Sample Size",
            min_value=1,
            max_value=100000,
            value=100,
            help="Number of rows to sample for AI analysis",
            key="ai_sample_size"
        )
    
    # AI Prompt input
    ai_prompt = st.text_area(
        "AI Analysis Prompt",
        value="""Based on the product reviews, list concisely what product features or aspects customers mention while expressing their satisfaction or dissatisfaction with a product. Return a list of aspects with short but meaningful names that can serve as labels for a classification model.""",
        height=150,
        help="Enter the prompt for AI analysis",
        key="ai_prompt"
    )
    
    # AI Analysis button
    run_ai_analysis = st.button("Run AI Analysis", type="primary", key="run_ai_analysis")
    
    # Execute AI analysis when button is clicked
    if run_ai_analysis and ai_table_name and text_column and ai_prompt:
        try:
            with st.spinner(f"Running AI analysis on {ai_sample_size} rows from {ai_table_name}.{text_column}..."):
                # Construct the AI_AGG query with sampling
                ai_query = f"""
                SELECT AI_AGG({text_column}, '''{ai_prompt}''')
                FROM {ai_table_name} SAMPLE ({ai_sample_size} ROWS)
                """
                
                # Execute the query
                ai_result = session.sql(ai_query).collect()
                
                if ai_result and len(ai_result) > 0:
                    # Get the result from the first row
                    analysis_result = ai_result[0][0]
                    
                    # Store result in session state for use in classification section
                    st.session_state.ai_analysis_result = analysis_result
                    
                    st.success("AI analysis completed successfully!")
                    
                    # Display the result as markdown
                    st.subheader("AI Analysis Results")
                    st.markdown(analysis_result)
                    
                    # Also show in an expandable code block for copying
                    with st.expander("Raw Output (for copying)"):
                        st.code(analysis_result, language=None)
                        
                else:
                    st.warning("No results returned from AI analysis")
                    
        except Exception as e:
            st.error(f"Error running AI analysis: {str(e)}")
            st.write("Please check that:")
            st.write("- The table name and column name are correct")
            st.write("- You have the necessary permissions to use AI functions")
            st.write("- The table contains data in the specified text column")
            st.write("- Your Snowflake account has AI features enabled")
    
    elif run_ai_analysis:
        missing_fields = []
        if not ai_table_name:
            missing_fields.append("Table Name")
        if not text_column:
            missing_fields.append("Text Column Name")
        if not ai_prompt:
            missing_fields.append("AI Analysis Prompt")
        
        st.warning(f"Please provide the following required fields: {', '.join(missing_fields)}")

# Section 3: AI Classification
with st.expander("🏷️ AI-Powered Text Classification", expanded=False):
    st.write("Extract categories from AI analysis results and classify text data using AI_CLASSIFY.")
    
    # Step 1: Extract Categories from Previous AI Analysis
    st.subheader("Step 1: Extract Categories")
    
    if st.session_state.ai_analysis_result:
        st.info("AI analysis result available from previous section.")
        with st.expander("View AI Analysis Result"):
            st.markdown(st.session_state.ai_analysis_result)
    else:
        st.warning("No AI analysis result found. Please run AI analysis in the previous section first.")
    
    extract_categories = st.button("Extract Categories", type="secondary", key="extract_categories")
    
    if extract_categories and st.session_state.ai_analysis_result:
        try:
            with st.spinner("Extracting categories using AI_COMPLETE..."):
                # Escape single quotes in the AI analysis result to prevent SQL syntax errors
                escaped_result = st.session_state.ai_analysis_result.replace("'", "''")
                
                # Construct the AI_COMPLETE query to extract categories
                extract_prompt = f"Please extract a list of categories, comma separated, from the following text. Return only the categories as a simple comma-separated list without any additional formatting or explanation: {escaped_result}"
                
                complete_query = f"SELECT AI_COMPLETE('claude-3-5-sonnet', '{extract_prompt}')"
                
                # Execute the query
                complete_result = session.sql(complete_query).collect()
                
                if complete_result and len(complete_result) > 0:
                    categories_text = complete_result[0][0]
                    
                    # Parse categories (split by comma and clean up)
                    categories_list = [cat.strip().strip('"').strip("'") for cat in categories_text.split(',')]
                    categories_list = [cat for cat in categories_list if cat]  # Remove empty strings
                    
                    # Add 'Other' category if not already present
                    if 'Other' not in categories_list and 'other' not in categories_list:
                        categories_list.append('Other')
                    
                    st.session_state.extracted_categories = categories_list
                    
                    st.success(f"Successfully extracted {len(categories_list)} categories (including 'Other')!")
                    st.write("**Extracted Categories:**")
                    for i, cat in enumerate(categories_list, 1):
                        st.write(f"{i}. {cat}")
                else:
                    st.error("No categories could be extracted")
                    
        except Exception as e:
            st.error(f"Error extracting categories: {str(e)}")
    
    elif extract_categories and not st.session_state.ai_analysis_result:
        st.warning("Please run AI analysis in the previous section first to extract categories.")
    
    # Step 2: AI Classification
    st.subheader("Step 2: Classify Text Data")
    
    # Classification parameters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        classify_table_name = st.text_input(
            "Table Name",
            value="VORTREX_REVIEWS",
            help="Enter the name of the table containing text data",
            key="classify_table_name"
        )
    
    with col2:
        classify_text_column = st.text_input(
            "Text Column Name",
            value="REVIEW_TEXT",
            help="Enter the name of the column containing text to classify",
            key="classify_text_column"
        )
    
    with col3:
        classify_sample_size = st.number_input(
            "Sample Size",
            min_value=1,
            max_value=10000,
            value=500,
            help="Number of rows to sample for classification",
            key="classify_sample_size"
        )
    
    # Show extracted categories if available
    if st.session_state.extracted_categories:
        st.write("**Categories to use for classification:**")
        categories_display = "', '".join(st.session_state.extracted_categories)
        st.code(f"['{categories_display}']", language=None)
        
        # Show category count
        st.info(f"Total categories: {len(st.session_state.extracted_categories)}")
        
        # Manual category editing option
        with st.expander("Edit Categories (Optional)"):
            categories_text = st.text_area(
                "Edit categories (one per line):",
                value="\n".join(st.session_state.extracted_categories),
                help="You can modify the categories here if needed",
                key="manual_categories"
            )
            
            if st.button("Update Categories", key="update_categories"):
                manual_categories = [cat.strip() for cat in categories_text.split('\n') if cat.strip()]
                st.session_state.extracted_categories = manual_categories
                st.rerun()
    
    # Classification button
    run_classification = st.button("Run Classification", type="primary", key="run_classification")
    
    # Execute classification when button is clicked
    if run_classification and classify_table_name and classify_text_column and st.session_state.extracted_categories:
        try:
            with st.spinner(f"Classifying {classify_sample_size} rows from {classify_table_name}..."):
                # Format categories for SQL query
                categories_sql = "', '".join(st.session_state.extracted_categories)
                categories_sql = f"['{categories_sql}']"
                
                # Construct the AI_CLASSIFY query
                classify_query = f"""
                SELECT 
                    {classify_text_column} as text,
                    COALESCE(GET(AI_CLASSIFY({classify_text_column}, {categories_sql}):labels::VARIANT, 0)::VARCHAR, 'Other') as classification_label
                FROM {classify_table_name} 
                SAMPLE ({classify_sample_size} ROWS)
                """
                
                # Execute the query
                classify_result = session.sql(classify_query).to_pandas()
                
                if not classify_result.empty:
                    st.success(f"Successfully classified {len(classify_result)} text samples!")
                    
                    # Store complete classification results in session state for outlier analysis
                    st.session_state.classification_results = classify_result
                    st.session_state.classification_table_info = {
                        'table_name': classify_table_name,
                        'text_column': classify_text_column,
                        'sample_size': classify_sample_size,
                        'categories': st.session_state.extracted_categories.copy()
                    }
                    
                    st.info("✅ Classification results stored in session for outlier analysis (no Snowflake tables created)")
                else:
                    st.warning("No classification results returned")
                    
        except Exception as e:
            st.error(f"Error running classification: {str(e)}")
            st.write("Please check that:")
            st.write("- The table name and column name are correct")
            st.write("- You have the necessary permissions to use AI functions")
            st.write("- The categories were extracted successfully")
            st.write("- Your Snowflake account has AI features enabled")
    
    elif run_classification:
        missing_items = []
        if not classify_table_name:
            missing_items.append("Table Name")
        if not classify_text_column:
            missing_items.append("Text Column Name")
        if not st.session_state.extracted_categories:
            missing_items.append("Extracted Categories (run Extract Categories first)")
        
        st.warning(f"Please provide: {', '.join(missing_items)}")
    
    # Display classification results if they exist in session state
    # This is separate from the button click logic so results persist across reruns
    if st.session_state.classification_results is not None:
        classify_result = st.session_state.classification_results
        
        # Display classification summary
        st.subheader("Classification Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Samples", len(classify_result))
        with col2:
            # Use uppercase column name as returned by Snowflake
            unique_labels = classify_result['CLASSIFICATION_LABEL'].nunique()
            st.metric("Unique Labels", unique_labels)
        with col3:
            most_common = classify_result['CLASSIFICATION_LABEL'].mode().iloc[0] if not classify_result.empty else "N/A"
            st.metric("Most Common Label", most_common)
        
        # Show label distribution
        st.subheader("Label Distribution")
        label_counts = classify_result['CLASSIFICATION_LABEL'].value_counts()
        
        # Bar chart for frequencies
        st.bar_chart(label_counts)
        
        # Display the classification results with filtering
        st.subheader("Classification Results")
        
        # Filter controls
        col1, col2 = st.columns([2, 1])
        with col1:
            # Multi-select filter for classification labels
            unique_labels = sorted(classify_result['CLASSIFICATION_LABEL'].unique())
            selected_labels = st.multiselect(
                "Filter by Classification Label:",
                options=unique_labels,
                default=unique_labels,
                key="label_filter"
            )
        
        with col2:
            # Show count of filtered results
            if selected_labels:
                filtered_count = len(classify_result[classify_result['CLASSIFICATION_LABEL'].isin(selected_labels)])
                st.metric("Filtered Results", filtered_count)
            else:
                st.metric("Filtered Results", 0)
        
        # Apply filter
        if selected_labels:
            filtered_result = classify_result[classify_result['CLASSIFICATION_LABEL'].isin(selected_labels)]
            
            st.dataframe(
                filtered_result[['TEXT', 'CLASSIFICATION_LABEL']], 
                use_container_width=True,
                height=400
            )
        else:
            st.warning("No labels selected. Please select at least one label to view results.")

# Section 4: Outlier Analysis
with st.expander("🔍 Outlier Analysis", expanded=False):
    st.write("Analyze texts classified as 'Other' to identify common patterns or themes.")
    
    # Check if we have classification results
    if 'classification_results' in st.session_state and st.session_state.classification_results is not None:
        # Check if there are any 'Other' classifications
        other_count = len(st.session_state.classification_results[
            st.session_state.classification_results['CLASSIFICATION_LABEL'] == 'Other'
        ])
        
        if other_count > 0:
            st.info(f"Found {other_count} texts classified as 'Other' - ready for outlier analysis.")
            
            # Outlier analysis parameters
            outlier_prompt = st.text_area(
                "Outlier Analysis Prompt",
                value="""Analyze the following texts that were classified as 'Other' (didn't fit into the main categories). 
Identify common patterns, themes, or characteristics that these texts share. 
What makes these texts different from the main categories? 
Are there potential new categories or subcategories that could be created?
Provide insights about why these texts might be outliers.""",
                height=150,
                help="Customize the prompt for analyzing outlier texts",
                key="outlier_prompt"
            )
            
            # Show sample of 'Other' texts from previous classification
            with st.expander("Preview 'Other' Texts (from previous classification)"):
                other_texts = st.session_state.classification_results[
                    st.session_state.classification_results['CLASSIFICATION_LABEL'] == 'Other'
                ]['TEXT'].head(5)
                for i, text in enumerate(other_texts, 1):
                    st.write(f"**{i}.** {text[:200]}...")
            
            # Run outlier analysis button
            run_outlier_analysis = st.button("Run Outlier Analysis", type="primary", key="run_outlier_analysis")
            
            if run_outlier_analysis and outlier_prompt and st.session_state.classification_table_info:
                try:
                    with st.spinner(f"Analyzing {other_count} outlier texts..."):
                        # Get the original classification parameters from session state
                        table_info = st.session_state.classification_table_info
                        classify_table_name = table_info['table_name']
                        classify_text_column = table_info['text_column']
                        classify_sample_size = table_info['sample_size']
                        categories = table_info['categories']
                        
                        # Format categories for SQL query
                        categories_sql = "', '".join(categories)
                        categories_sql = f"['{categories_sql}']"
                        
                        # Escape single quotes in the prompt
                        escaped_prompt = outlier_prompt.replace("'", "''")
                        
                        # Run AI_CLASSIFY in a subquery, filter for 'Other', then run AI_AGG on those texts
                        # Note that in a regular pipeline you would not run AI_CLASSIFY twice in the same query, but here we do this to avoid saving a temporary table in Snowflake.
                        outlier_query = f"""
                        WITH classified_texts AS (
                            SELECT 
                                {classify_text_column} as text,
                                COALESCE(GET(AI_CLASSIFY({classify_text_column}, {categories_sql}):labels::VARIANT, 0)::VARCHAR, 'Other') as classification_label
                            FROM {classify_table_name} 
                            SAMPLE ({classify_sample_size} ROWS)
                        )
                        SELECT AI_AGG(text, '''{escaped_prompt}''')
                        FROM classified_texts
                        WHERE classification_label = 'Other'
                        """
                        
                        # Execute the query
                        outlier_result = session.sql(outlier_query).collect()
                        
                        if outlier_result and len(outlier_result) > 0:
                            # Get the result from the first row
                            outlier_analysis = outlier_result[0][0]
                            
                            st.success("Outlier analysis completed successfully!")
                            
                            # Display the result as markdown
                            st.subheader("Outlier Analysis Results")
                            st.markdown(outlier_analysis)
                            
                            # Also show in an expandable code block for copying
                            with st.expander("Raw Output (for copying)"):
                                st.code(outlier_analysis, language=None)
                                
                            # Store result in session state
                            st.session_state.outlier_analysis_result = outlier_analysis
                                
                        else:
                            st.warning("No results returned from outlier analysis")
                            
                except Exception as e:
                    st.error(f"Error running outlier analysis: {str(e)}")
                    st.write("Please check that:")
                    st.write("- The temporary table exists and contains data")
                    st.write("- You have the necessary permissions to use AI functions")
                    st.write("- There are texts classified as 'Other'")
            
            elif run_outlier_analysis and not outlier_prompt:
                st.warning("Please provide an outlier analysis prompt")
                
        else:
            st.info("No texts were classified as 'Other'. All texts fit into the defined categories.")
            
    else:
        st.warning("No classification results found. Please run classification in the previous section first.")
