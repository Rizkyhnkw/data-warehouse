/*
====================================================================================================
--     Loads data from the Bronze layer to the Silver layer.
--     This procedure cleanses, standardizes, and transforms the raw data.
--     It follows a full-refresh (DELETE/INSERT) pattern for each target table.
====================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    -- Declare variables for performance tracking
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    -- Use a TRY...CATCH block for robust error handling during the entire load process
    BEGIN TRY
        -- Mark the start time for the entire batch load
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- ==== 1. Load Customer Information (crm_cust_info) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.crm_cust_info';
        DELETE FROM silver.crm_cust_info; -- Use DELETE for safety with foreign keys
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            trim(cst_firstname) as cst_firstname,
            trim(cst_lastname) as cst_lastname,
            -- Standardize marital status from single characters to full words
            case
                when upper(trim(cst_marital_status)) = 'S' then 'Single'
                when upper(trim(cst_marital_status)) = 'M' then 'Married'
                else 'n/a'
            end as cst_marital_status,
            -- Standardize gender from single characters to full words
            case
                when upper(trim(cst_gndr)) = 'F' then 'Female'
                when upper(trim(cst_gndr)) = 'M' then 'Male'
                else 'n/a'
            end as cst_gndr,
            cst_create_date
        FROM (
            -- Deduplication step: Identify the latest record for each customer ID
            select *,
            ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
            from bronze.crm_cust_info
            where cst_id is not null
        ) t
        where flag_last = 1; -- Filter to keep only the most recent record
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- ==== 2. Load Product Information (crm_prd_info) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.crm_prd_info';
        DELETE FROM silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
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
            prd_id,
            -- Parse the product key to extract the category ID
            replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
            -- Parse the product key to extract the clean product key
            SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
            prd_nm,
            -- Handle null values for product cost, defaulting to 0
            isnull(prd_cost, 0) as prd_cost,
            -- Standardize product line from single characters to full words
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'T' THEN 'Touring'
                WHEN 'S' THEN 'Other sales'
                ELSE 'n/a'
            END AS prd_line,
            -- Ensure start date is in DATE format
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            -- Calculate the effective end date for Slowly Changing Dimension (SCD) Type 2
            -- The end date is the day before the next record's start date
            CAST(LEAD(prd_start_dt, 1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- ==== 3. Load Sales Details (crm_sales_details) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.crm_sales_details';
        DELETE FROM silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
            sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT
            [sls_ord_num],
            [sls_prd_key],
            [sls_cust_id],
            -- Validate and cast integer dates (e.g., 20230115) to DATE format
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE) END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE) END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE) END AS sls_due_dt,
            -- Validate sales amount. If invalid or null, recalculate it from quantity and price.
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales,
            [sls_quantity],
            -- Validate price. If invalid or null, recalculate it from sales and quantity.
            CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- ==== 4. Load ERP Customer Data (erp_cust_az12) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.erp_cust_az12';
        DELETE FROM silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT
            -- Clean customer ID by removing 'NAS' prefix if it exists
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            -- Validate birth date, setting future dates to NULL
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            -- Standardize gender from various formats to a consistent value
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- ==== 5. Load ERP Location Data (erp_loc_a101) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.erp_loc_a101';
        DELETE FROM silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
            -- Clean customer ID by removing hyphens
            REPLACE(cid, '-', '') as cid,
            -- Standardize country names from codes/abbreviations to full names
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- ==== 6. Load ERP Category Data (erp_px_cat_g1v2) ====
        SET @start_time = GETDATE();
        PRINT '>> Deleting Data From: silver.erp_px_cat_g1v2';
        DELETE FROM silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        -- This is a direct pass-through load with no transformations needed
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM [bronze].[erp_px_cat_g1v2];
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Mark the end time for the entire batch and print total duration
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        -- If an error occurs, print detailed error information
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
