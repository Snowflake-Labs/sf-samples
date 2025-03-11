-- CREATE TEXT CHUNKING FUNCTION
-- This function reads a PDF file from a given URL, extracts the text, and splits it into chunks.
-- It returns a table with two columns: chunk_order (the order of the chunk in the document) and chunk (the text of the chunk).

create or replace function pdf_text_chunker(file_url string)
returns table (chunk_order NUMBER, chunk varchar)
language python
runtime_version = '3.9'
handler = 'pdf_text_chunker'
packages = ('snowflake-snowpark-python','PyPDF2', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging
import pandas as pd

class pdf_text_chunker:

    def read_pdf(self, file_url: str) -> str:
    
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
    
        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.readall())
            
        reader = PyPDF2.PdfReader(buffer)   
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"
                logger.warn(f"Unable to extract from file {file_url}, page {page}")
        
        return text

    def process(self,file_url: str):

        text = self.read_pdf(file_url)
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 5000, #Adjust this as you see fit
            chunk_overlap  = 500, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(text)
        # Add a sequence number for each chunk
        chunks_with_order = [(index, chunk) for index, chunk in enumerate(chunks, start=1)]
        df = pd.DataFrame(chunks_with_order, columns=['chunk_order', 'chunk'])
        
        yield from df.itertuples(index=False, name=None)
$$;

-- CREATE STAGE
-- This creates a stage named 'rag' for storing PDF files. It uses Snowflake's built-in encryption and enables directory listing.
create or replace stage rag ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

-- CHECK RAG AFTER LOADING
-- This command lists the files in the 'rag' stage to verify that the PDFs have been loaded correctly.

ls @rag;

-- CREATE TABLE OF CHUNKS + EMBEDDINGS WITH CHUNK ORDER
-- This creates a table named 'CHUNKS_TABLE' to store the text chunks and their embeddings.
-- The table has columns for the relative path, size, URL, scoped URL, chunk order, chunk text, and chunk embedding vector.

CREATE OR REPLACE TABLE CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK_ORDER NUMBER(38,0), -- Order of the chunk in the document
    CHUNK VARCHAR(16777216), -- Piece of text
    CHUNK_VEC VECTOR(FLOAT, 768)  -- Embedding using the VECTOR data type
);

-- CHECK CHUNK & EMBEDDING TABLE
-- This query selects all columns from the 'CHUNKS_TABLE' to verify that the table has been created correctly.
SELECT * 
FROM CHUNKS_TABLE;

-- INSERT DATA INTO CHUNKS_TABLE
-- This query inserts data into the 'CHUNKS_TABLE' by selecting files from the 'rag' stage, passing them through the 'pdf_text_chunker' function,
-- and inserting the resulting chunks and their embeddings into the table.
insert into chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk_order, chunk, chunk_vec)
    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@rag, relative_path) as scoped_file_url,
            func.chunk_order,
            func.chunk as chunk,
            snowflake.cortex.embed_text('e5-base-v2',chunk) as chunk_embeddings
            --null as chunk_vec
    from 
        directory(@rag),
        TABLE(pdf_text_chunker(build_scoped_file_url(@rag, relative_path))) as func;

-- CHECK CHUNKS TABLE WITH DATA
-- These queries select data from the 'CHUNKS_TABLE' to verify that the data has been inserted correctly.
-- The first query selects all columns and calculates the character length of each chunk.
-- The second query groups the data by relative path and counts the number of chunks and total size for each file.

select *
        ,length(chunk) as character_length
from chunks_table;

select relative_path
        ,count(*) as num_chunks
        ,sum(size) as size
    from chunks_table
    group by relative_path;