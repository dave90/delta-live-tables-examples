CREATE OR REPLACE TABLE review_score
AS SELECT * FROM delta.`dbfs:/FileStore/tables/dlt_streaming/tables/reviewer_score`;

SELECT * FROM review_score  LIMIT 10;