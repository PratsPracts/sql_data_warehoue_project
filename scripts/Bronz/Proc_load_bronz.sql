/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


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
