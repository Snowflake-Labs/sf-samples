-- Scrape Financial Times for Articles
CREATE OR REPLACE FUNCTION SNOW_INVEST.BRONZE.scrape_financial_times()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'scrape_financial_times'
EXTERNAL_ACCESS_INTEGRATIONS = (ft_integration)
PACKAGES = ('requests', 'beautifulsoup4')
AS
$$
import requests
from bs4 import BeautifulSoup
import json

def scrape_financial_times():
    url = 'https://www.ft.com/technology'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []

    for article in soup.find_all('div', class_='o-teaser__heading'):
        title_tag = article.find('a')
        if title_tag:
            title = title_tag.text.strip()
            link = 'https://www.ft.com' + title_tag['href']
            articles.append({'title': title, 'link': link})

    return json.dumps(articles)
$$;

-- Creating a View for Financial Times Articles
CREATE OR REPLACE VIEW SNOW_INVEST.BRONZE.FINANCIALTIMES_ARTICLES_WH AS
SELECT scrape_financial_times() AS data;

-- Synthesis View Based on Financial Times Analysis
CREATE OR REPLACE VIEW SNOW_INVEST.BRONZE.FT_WH_RESPONSE AS
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mixtral-8x7b',
    CONCAT(
        'Prompt: Objective is to provide a synthesis of recommendations based on the analysis of recent Financial Times articles focusing on Wealth Management...',
        'Response: '
        /* Complete the prompt as necessary and ensure it does not exceed 32000 tokens. */
    )
) AS response
FROM SNOW_INVEST.BRONZE.FINANCIALTIMES_ARTICLES_WH;

create or replace view SNOW_INVEST.SILVER.FINANCIALTIMES_ARTICLES_STRUCTURED(
	TITLE,
	LINK
) as
SELECT
    f.value:title::STRING AS title,
    CASE
        WHEN f.value:link::STRING LIKE 'https://www.ft.com/content/%' THEN
            f.value:link::STRING
        ELSE
            REPLACE(f.value:link::STRING, 'https://www.ft.com', '')
    END AS link
FROM tikehau.bronze.financialtimes_articles_wh,
LATERAL FLATTEN(input => PARSE_JSON(tikehau.bronze.financialtimes_articles_wh.data)) f;
