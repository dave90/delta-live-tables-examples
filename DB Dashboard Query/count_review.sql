CREATE OR REPLACE TABLE TOP_REVIEW
AS SELECT * FROM delta.`dbfs:/FileStore/tables/dlt_streaming/tables/top_review`;

SELECT COUNT(*) FROM top_review;
