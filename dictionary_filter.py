import duckdb
from functools import lru_cache
import pymorphy3

morph = pymorphy3.MorphAnalyzer()

# restrict cache to increase calculating speed 
@lru_cache(maxsize=500000)

# LEMMATIZING
def get_lemma(word):
    return morph.parse(word)[0].normal_form

def lemmatize_tokens_udf(tokens):
    if not tokens:
        return []
    return [get_lemma(token) for token in tokens]

conn = duckdb.connect()

# Specify type to optimize calculating 
conn.create_function(
    "lemmatize_tokens", 
    lemmatize_tokens_udf, 
    [duckdb.list_type(str)], 
    duckdb.list_type(str)
)

# SQL QUERY 
conn.execute("""
    CREATE OR REPLACE TABLE processed_data AS 
    
    -- Step 1: Text cleaning (the same procedure for both columns)
    WITH cleaned AS (
        SELECT 
            project_short_name,
            LOWER(TRIM(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                COALESCE(question, ''),
                                '^/[a-zA-Z0-9_]+|\\s/[a-zA-Z0-9_]+', '' -- Deleting bot comands (/start, /help)
                            ),
                            '&[a-zA-Z0-9#]+;', ' ' -- Deleting HTML (&nbsp;, &#123;)
                        ),
                        'https?://\\S+|www\\.\\S+', '' -- Deleting links 
                    ),
                    '\\s+', ' ' -- Working with spaces 
                )
            )) AS question,
            
            LOWER(TRIM(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                COALESCE(answer, ''),
                                '^/[a-zA-Z0-9_]+|\\s/[a-zA-Z0-9_]+', ''
                            ),
                            '&[a-zA-Z0-9#]+;', ' '
                        ),
                        'https?://\\S+|www\\.\\S+', ''
                    ),
                    '\\s+', ' '
                )
            )) AS answer
        FROM read_parquet('chatbots_dataset.parquet') -- Here we download our dataset 
    ),
    
    -- Step 2: DUCKDB tokenization 
    tokenized AS (
        SELECT 
            *,
            regexp_extract_all(question, '[а-яёa-z0-9_]+(?:-[а-яёa-z0-9_]+)*') AS question_tokens,
            regexp_extract_all(answer, '[а-яёa-z0-9_]+(?:-[а-яёa-z0-9_]+)*') AS answer_tokens
        FROM cleaned
    )
    
    -- Step 3: Lemmatization
    SELECT 
        project_short_name,
        question,
        answer,
        question_tokens,
        answer_tokens,
        lemmatize_tokens(question_tokens) AS question_lemmas,
        lemmatize_tokens(answer_tokens) AS answer_lemmas
    FROM tokenized
""")


## DICTIONARY SEARCHING 

conn.execute("""
    CREATE OR REPLACE TEMP TABLE filter_words AS
    SELECT list(LOWER(TRIM(normalized_term))) as keyword_list
    FROM read_csv_auto('illegal_terms_dictionary.csv')
    WHERE normalized_term IS NOT NULL AND TRIM(normalized_term) != ''
""")

conn.execute("""
    CREATE OR REPLACE TABLE filtered_data AS 
    WITH target AS (
        SELECT keyword_list FROM filter_words LIMIT 1
    )
    SELECT pd.*
    FROM processed_data pd, target t
    WHERE 
        -- Intersection is more than 0 
        len(list_intersect(pd.question_lemmas, t.keyword_list)) > 0
        OR len(list_intersect(pd.answer_lemmas, t.keyword_list)) > 0
""")


# Statistics 
original_count = conn.execute("SELECT COUNT(*) FROM processed_data").fetchone()[0]
filtered_count = conn.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]

print(f"Original number of rows: {original_count:,}")
print(f"Intersection with dictionary: {filtered_count:,}")

conn.execute("""
    COPY filtered_data TO 'filtered_chatbots_dataset.parquet' (FORMAT PARQUET)
""")
