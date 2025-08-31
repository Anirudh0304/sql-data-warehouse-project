CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    --------------------------------------------------------------------
    -- CRM Tables
    --------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- crm_cust_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr, 
        cst_create_date
    )
    SELECT
        b.cst_id,
        b.cst_key,
        TRIM(b.cst_firstname),
        TRIM(b.cst_lastname),
        CASE 
            WHEN UPPER(TRIM(b.cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(b.cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(b.cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(b.cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        b.cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) b
    WHERE b.flag_last = 1;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- crm_prd_info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        b.prd_id,
        REPLACE(SUBSTRING(b.prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(b.prd_key, 7) AS prd_key,
        b.prd_nm,
        COALESCE(b.prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(b.prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(b.prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(b.prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(b.prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        b.prd_start_dt::DATE,
        (LEAD(b.prd_start_dt) OVER (PARTITION BY SUBSTRING(b.prd_key, 7) ORDER BY b.prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt
    FROM bronze.crm_prd_info b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- crm_sales_details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        b.sls_ord_num,
        b.sls_prd_key,
        b.sls_cust_id,
        CASE 
            WHEN b.sls_order_dt = 0 OR LENGTH(b.sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(b.sls_order_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN b.sls_ship_dt = 0 OR LENGTH(b.sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(b.sls_ship_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN b.sls_due_dt = 0 OR LENGTH(b.sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(b.sls_due_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN b.sls_sales IS NULL OR b.sls_sales <= 0 OR b.sls_sales != b.sls_quantity * ABS(b.sls_price) 
                THEN b.sls_quantity * ABS(b.sls_price)
            ELSE b.sls_sales
        END,
        b.sls_quantity,
        CASE 
            WHEN b.sls_price IS NULL OR b.sls_price <= 0 
                THEN b.sls_sales / NULLIF(b.sls_quantity, 0)
            ELSE b.sls_price
        END
    FROM bronze.crm_sales_details b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


    --------------------------------------------------------------------
    -- ERP Tables
    --------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- erp_cust_az12
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN b.cid LIKE 'NAS%' THEN SUBSTRING(b.cid, 4)
            ELSE b.cid
        END,
        CASE
            WHEN b.bdate > CURRENT_DATE THEN NULL
            ELSE b.bdate
        END,
        CASE
            WHEN UPPER(TRIM(b.gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(b.gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12 b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- erp_loc_a101
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(b.cid, '-', '') AS cid,
        CASE
            WHEN TRIM(b.cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(b.cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(b.cntry) = '' OR b.cntry IS NULL THEN 'n/a'
            ELSE TRIM(b.cntry)
        END
    FROM bronze.erp_loc_a101 b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- erp_px_cat_g1v2
    start_time := CURRENT_TIMESTAMP;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        b.id,
        b.cat,
        b.subcat,
        b.maintenance
    FROM bronze.erp_px_cat_g1v2 b;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    --------------------------------------------------------------------
    -- Wrap up
    --------------------------------------------------------------------
    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;
