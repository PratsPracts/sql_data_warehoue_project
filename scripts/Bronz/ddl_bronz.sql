/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

use master;
create database DataWarehouse;
Use DataWarehouse;

create schema Bronz;
Go
create schema Silver;
Go
create schema Gold;
Go
--------------------------

CREATE TABLE bronz.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronz.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronz.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronz.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronz.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronz.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
);
GO

exec bronz.load_bronz

Create or alter procedure bronz.load_bronz As
Begin
    Declare @start_time DateTime , @End_Time Datetime , @batch_start_time datetime, @batch_end_time datetime;
    Begin Try
	set @batch_start_time = getdate();
    Print '====================================';
    Print 'Loading Bronz Layer';
    Print '====================================';

    Print '***********************************';
    Print 'Loading CRM TABLE';
    Print '***********************************';

	set @start_time = getdate();
    Print '>> Truncating Table:bronz.crm_cust_info >>';
    Truncate table bronz.crm_cust_info;
    Print '>>Inserting Data into : bronz.crm_cust_info >>';
    Bulk Insert bronz.crm_cust_info
    from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
      with (
            firstRow = 2,
            Fieldterminator = ',',
            tablock
            );
	  set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>';

    set @start_time = getdate();
    Print '>> Truncating Table:bronz.crm_prd_info >>';
    Truncate table bronz.crm_prd_info;
    Print '>>Inserting Data into : bronz.crm_prd_info >>';
    Bulk Insert bronz.crm_prd_info
    from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (
             firstRow = 2,
             Fieldterminator = ',',
             tablock
             );
	set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>';

   set @start_time = getdate();
   Print '>> Truncating Table:bronz.crm_sales_details >>';
   Truncate table bronz.crm_sales_details;
   Print '>>Inserting Data into : bronz.crm_sales_details >>';
   Bulk Insert bronz.crm_sales_details
   from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
         with (
              firstRow = 2,
              Fieldterminator = ',',
              tablock
              );
			  set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>';

    Print '***********************************';
    Print 'Loading ERP TAble';
    Print '***********************************';

	set @start_time = getdate();
    Print '>> Truncating Table:bronz.erp_cust_az12 >>';
    Truncate table bronz.erp_cust_az12;
    Print '>>Inserting Data into : bronz.erp_cust_az12 >>';
    Bulk Insert bronz.erp_cust_az12
    from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    with (
         firstRow = 2,
         Fieldterminator = ',',
         tablock
         );
		 set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>';

    set @start_time = getdate();
	Print '>> Truncating Table:bronz.erp_loc_a101 >>';
    Truncate table bronz.erp_loc_a101;
    Print '>>Inserting Data into : bronz.erp_loc_a101 >>';
    Bulk Insert bronz.erp_loc_a101
    from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
      with (
           firstRow = 2,
           Fieldterminator = ',',
           tablock
           );
		   set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>';
    
	set @start_time = getdate();
    Print '>> Truncating Table:bronz.erp_px_cat_g1v2 >>';
    Truncate table bronz.erp_px_cat_g1v2;
    Print '>>Inserting Data into : bronz.erp_px_cat_g1v2 >>';
    Bulk Insert bronz.erp_px_cat_g1v2
    from 'C:\Users\Shree\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
         with (
              firstRow = 2,
              Fieldterminator = ',',
              tablock
              );
			  set @end_time = Getdate();
	  Print '>> Load Duration: ' + cast (DateDiff(second, @start_time, @end_time) AS Nvarchar) + 'second';
	  Print '>>--------------------------->>'; 

      set @batch_end_time = getdate();
	  Print '======================================';
	  Print ' Loading Bronz layer is completed';
	  Print 'Total Load Duration: ' + cast (DateDiff(second, @batch_start_time, @batch_end_time) AS Nvarchar) + 'second';
	  Print '===========================================';
   End Try 
   begin catch
      print '-----------------------';
	  Print 'Error occured during Loading Broze Layer';
	  Print ' Error message' + Error_message();
	  Print ' Error message' + Cast (Error_number() as Nvarchar);
	  Print ' Error message' + Cast (Error_state() as Nvarchar);
	  Print '-----------------------';
   end catch
End
