# DuckDB searching of intersections with a prescribed dictionary
This code is aimed to find intersections with a dictionary of illegal words in the dataset of chatbot logs having 8 million rows. Traditional methods like using NLTK or SpaCy along with Pandas turned out to be too uneffective for quite large data. Therefore I decided to apply SQL queries using DuckDB.

## How it works 
This script expects 2 additional files:
* your dataset in Parquete format,
* your dictionary in CSV format.

To personalize the query you have to change the following places in your code:
```python
 SELECT 
            your_additional_column_name_if_needed,
 COALESCE(Your_column_for_intersection, ''),
 AS name_of_your_column_for_intersection
 FROM read_parquet('your_dataset.parquet')
```
```python
CREATE OR REPLACE TEMP TABLE filter_words AS
    SELECT list(LOWER(TRIM(name_of_your_column))) as keyword_list
    FROM read_csv_auto('your_dictionary.csv')
    WHERE name_of_your_column IS NOT NULL AND TRIM(name_of_your_column) != ''
```
