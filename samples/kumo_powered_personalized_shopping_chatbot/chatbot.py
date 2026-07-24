import streamlit as st
from snowflake.core import Root # requires snowflake>=0.8.0
from snowflake.snowpark.context import get_active_session

MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

def init_messages():
    """
    Initialize the session state for chat messages. If the session state indicates that the
    conversation should be cleared or if the "messages" key is not in the session state,
    initialize it as an empty list.
    """
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def init_customer_id():
    """
    Initialize or reset the customer ID in the session state.
    
    """
    if "customer_id" not in st.session_state:
        st.session_state.customer_id = None

def init_service_metadata():
    """
    Initialize the session state for cortex search service metadata. Query the available
    cortex search services from the Snowflake session and store their names and search
    columns in the session state.
    """
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                svc_search_col = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {svc_name};"
                ).collect()[0]["search_column"]
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )

        st.session_state.service_metadata = service_metadata

def init_config_options():
    """
    Initialize the configuration options in the Streamlit sidebar. Allow the user to select
    a cortex search service, clear the conversation, toggle debug mode, and toggle the use of
    chat history. Also provide advanced options to select a model, the number of context chunks,
    and the number of chat messages to use in the chat history.
    """
    # st.sidebar.selectbox(
    #     "Select cortex search service:",
    #     [s["name"] for s in st.session_state.service_metadata],
    #     key="selected_cortex_search_service",
    # )

    st.sidebar.button("Clear conversation", key="clear_conversation")
    st.sidebar.toggle("Debug", key="debug", value=False)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=True)

    with st.sidebar.expander("Advanced options"):
        st.selectbox("Select model:", MODELS, key="model_name")
        st.number_input(
            "Select number of context chunks",
            value=5,
            key="num_retrieved_chunks",
            min_value=1,
            max_value=10,
        )
        st.number_input(
            "Select number of messages to use in chat history",
            value=5,
            key="num_chat_messages",
            min_value=1,
            max_value=10,
        )

    st.sidebar.expander("Session State").write(st.session_state)

def add_login_feature():
    """
    Add a login feature to input and persist the customer ID.
    
    """

    with st.sidebar:
        st.header("Customer Login")
        # Customer ID input
        customer_id = st.text_input(
            "Enter Customer ID",
            value=st.session_state.customer_id or "",
            placeholder="input ID here"
        )
        
        # Update customer ID in session state
        if st.button("Log In"):
            if customer_id.strip():  # Ensure input is not empty
                st.session_state.customer_id = customer_id.strip()
                st.success(f"Logged in as Customer ID: {st.session_state.customer_id}")
            else:
                st.error("Customer ID cannot be empty.")
        
        # Logout option
        if st.session_state.customer_id:
            if st.button("Log Out"):
                st.session_state.customer_id = None
                st.info("Logged out.")



def query_cortex_search_service(query, selected_service_name):
    """
    Query the selected cortex search service with the given query and retrieve context documents.
    Display the retrieved context documents in the sidebar if debug mode is enabled. Return the
    context documents as a string.

    Args:
        query (str): The query to search the cortex search service with.

    Returns:
        str: The concatenated string of context documents.
    """
    db, schema = session.get_current_database(), session.get_current_schema()

    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services[selected_service_name]
    )

    context_documents = cortex_search_service.search(
        query, columns=[], limit=st.session_state.num_retrieved_chunks
    )
    results = context_documents.results

    service_metadata = st.session_state.service_metadata
    search_col = next(
        (s["search_column"] for s in service_metadata if s["name"] == selected_service_name),
        None
    )
    if not search_col:
        raise ValueError(f"Invalid service name: {selected_service_name}")


    context_str = ""
    for i, r in enumerate(results):
        context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"

    if st.session_state.debug:
        st.sidebar.text_area(f"Context documents from {selected_service_name}", context_str, height=500)

    return context_str

def get_chat_history():
    """
    Retrieve the chat history from the session state limited to the number of messages specified
    by the user in the sidebar options.

    Returns:
        list: The list of chat messages from the session state.
    """
    start_index = max(
        0, len(st.session_state.messages) - st.session_state.num_chat_messages
    )
    return st.session_state.messages[start_index : len(st.session_state.messages) - 1]

def complete(model, prompt):
    """
    Generate a completion for the given prompt using the specified model.

    Args:
        model (str): The name of the model to use for completion.
        prompt (str): The prompt to generate a completion for.

    Returns:
        str: The generated completion.
    """
    return session.sql("SELECT snowflake.cortex.complete(?,?)", (model, prompt)).collect()[0][0]

def classify_query(model, query):
    """
        Generates classification for given query using the specified model.

        Args:
            model(str): The name of the model to use for classification
            prompt(str): The prompt to generate the classification for

        Returns:
            str: The generated classification
    """
    classification_prompt = f"""Please classify the given user query as either item to item
                                or item to user. If classifying as item to item, print "ITEM" with
                                no additional text, and no whitespace. If classifying as item to user, print "CUSTOMER"
                                with no additional text, and no whitespace. If the prompt involves a customer id, it will be
                                an item to user prompt. If the prompt involves item ids, it will be an
                                item to item prompt.
                                Here is the user query: {query}
                                Please return the appropriate classfication.
                                
                            """
    #st.write(classification_prompt)
    return complete(model, classification_prompt)




def make_chat_history_summary(chat_history, question):
    """
    Generate a summary of the chat history combined with the current question to extend the query
    context. Use the language model to generate this summary.

    Args:
        chat_history (str): The chat history to include in the summary.
        question (str): The current user question to extend with the chat history.

    Returns:
        str: The generated summary of the chat history and question.
    """
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """

    summary = complete(st.session_state.model_name, prompt)

    if st.session_state.debug:
        st.sidebar.text_area(
            "Chat history summary", summary.replace("$", "\$"), height=150
        )

    return summary

def create_prompt(user_question, selected_service_name):
    """
    Create a prompt for the language model by combining the user question with context retrieved
    from the cortex search service and chat history (if enabled). Format the prompt according to
    the expected input format of the model.

    Args:
        user_question (str): The user's question to generate a prompt for.

    Returns:
        str: The generated prompt for the language model.
    """

    customer_id = st.session_state.customer_id
    if customer_id:
        user_question = f"[Customer ID: {customer_id}]{user_question}"

    
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history != []:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context = query_cortex_search_service(question_summary, selected_service_name)
        else:
            prompt_context = query_cortex_search_service(user_question, selected_service_name)
    else:
        prompt_context = query_cortex_search_service(user_question, selected_service_name)
        chat_history = ""

    prompt = f"""
            [INST]
            You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
            you will also be given context provided between <context> and </context> tags. Use that context
            with the user's chat history provided in the between <chat_history> and </chat_history> tags
            to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
            and directly relevant to the user's question.

            If the user asks a generic question which cannot be answered with the given context or chat_history,
            just say "I don't know the answer to that question.

            Don't saying things like "according to the provided context".

            <chat_history>
            {chat_history}
            </chat_history>
            <context>
            {prompt_context}
            </context>
            <question>
            {user_question}
            </question>
            [/INST]
            Answer:
        """
    return prompt

def main():
    st.title(f":speech_balloon: Personalized Shopping Assistant Powered by Kumo")

    init_customer_id()
    add_login_feature()
    init_service_metadata()
    init_config_options()
    init_messages()

    icons = {"assistant": "❄️", "user": "👤"}

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            st.markdown(message["content"])

    disable_chat = (
        "service_metadata" not in st.session_state
        or len(st.session_state.service_metadata) == 0
    )
    if question := st.chat_input("Ask a question...", disabled=disable_chat):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user", avatar=icons["user"]):
            st.markdown(question.replace("$", "\$"))

        # Display assistant response in chat message container
        with st.chat_message("assistant", avatar=icons["assistant"]):
            message_placeholder = st.empty()
            question = question.replace("'", "")
            with st.spinner("Thinking..."):
                #classification response
                classification = classify_query(st.session_state.model_name, question)
                classification = classification.strip()
                #st.write(repr(classification))
                selected_search = ""
                #st.write(selected_search)
                if classification == "CUSTOMER":
                    #st.write("reached here")
                    selected_search = "ITEM_TO_USER_SERVICE"
                elif classification == "ITEM":
                    selected_search = "ITEM_TO_ITEM_SERVICE"
                elif classification == None:
                    selected_search = None
                #st.write(selected_search)
                generated_response = ""
                if classification is None:
                    generated_response = "I could not classify the query. Please try again."
                else:
                    generated_response = complete(
                        st.session_state.model_name, create_prompt(question, selected_search)
                    )
                message_placeholder.markdown(generated_response)

        st.session_state.messages.append(
            {"role": "assistant", "content": generated_response}
        )

if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()