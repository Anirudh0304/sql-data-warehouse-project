CREATE OR REPLACE PROCEDURE bronze.load_bronze(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    start_time timestamp;
    end_time timestamp;
    batch_start_time timestamp;
    batch_end_time timestamp;
BEGIN
    batch_start_time := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Copying Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info 
    FROM 'C:/temp/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));
    RAISE NOTICE '>> -------------';

	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info 
    FROM 'C:/temp/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));

	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details 
    FROM 'C:/temp/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));

	RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101 
    FROM 'C:/temp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));

	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 
    FROM 'C:/temp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));

	start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 
    FROM 'C:/temp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM (end_time - start_time));

	batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(epoch FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'SQLSTATE: %, MESSAGE: %', SQLSTATE, SQLERRM;
        RAISE NOTICE '==========================================';
END;
$BODY$;
