/*
===============================================================================
Stored Procedure: Load Bronze Layer with Data
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema table from external CSV file.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start TIMESTAMP;
    v_batch_end TIMESTAMP;
	v_row_count BIGINT;
BEGIN
    v_batch_start := clock_timestamp();

    RAISE NOTICE '======================================';
    RAISE NOTICE 'LOADING BRONZE LAYER';
    RAISE NOTICE '======================================';

    RAISE NOTICE '>> Truncating bronze.raw_loans';

    v_start_time := clock_timestamp();

    EXECUTE 'TRUNCATE TABLE bronze.raw_loans';

    v_end_time := clock_timestamp();

    RAISE NOTICE 'Table truncated successfully';

    RAISE NOTICE '>> Loading data into bronze.raw_loans';

    v_start_time := clock_timestamp();

    COPY bronze.raw_loans
    FROM 'C:/datasets/accepted_2007_to_2018Q4.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    v_end_time := clock_timestamp();

    RAISE NOTICE 'Load completed';

	GET DIAGNOSTICS v_row_count = ROW_COUNT;

	RAISE NOTICE 'Rows loaded: %', v_row_count;

    v_batch_end := clock_timestamp();

    RAISE NOTICE '======================================';
    RAISE NOTICE 'BRONZE LOAD COMPLETED';
    RAISE NOTICE '======================================';

    RAISE NOTICE 'Duration (sec): %',
        EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '======================================';
        RAISE NOTICE 'ERROR DURING BRONZE LOAD';
        RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
        RAISE NOTICE '======================================';
END;
$$;

CALL bronze.load_bronze();