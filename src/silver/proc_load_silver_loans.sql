/*
===============================================================================
Stored Procedure: Load Silver Layer
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to 
    populate the 'silver.loan_data' table from the 'bronze.raw_loans' table.

    Actions Performed:
        - Truncates the Silver table to ensure a fresh load.
        - Casts data from TEXT to appropriate types (INT, NUMERIC).
        - Cleanses data by trimming whitespace and handling empty strings (NULLIF).
        - Implements exception handling to capture and log any loading errors.
		
Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
BEGIN
    v_batch_start_time := clock_timestamp();
    
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.loan_data';
    TRUNCATE TABLE silver.loan_data;

    v_end_time := clock_timestamp();
	
	BEGIN
	    RAISE NOTICE '>> Inserting Data Into: silver.loan_data';
	    INSERT INTO silver.loan_data (
	        loan_id, 
			loan_amnt, 
			funded_amnt, 
			funded_amnt_inv, 
			term, 
			int_rate, 
	        installment, 
			grade, 
			sub_grade, 
			emp_length, 
			home_ownership, 
	        annual_inc, 
			issue_d, 
			loan_status, 
			purpose, 
			addr_state, 
			dti, 
	        delinq_2yrs, 
			earliest_cr_line, 
			fico_range_low, 
			fico_range_high, 
	        inq_last_6mths, 
			mths_since_last_delinq, 
			open_acc, 
			pub_rec, 
	        revol_bal, 
			revol_util, 
			total_acc, 
			out_prncp, 
			out_prncp_inv, 
	        total_pymnt, 
			total_rec_prncp, 
			total_rec_int, 
			total_rec_late_fee, 
	        recoveries, 
			collection_recovery_fee, 
			last_pymnt_d, 
			last_pymnt_amnt, 
	        last_credit_pull_d, 
			last_fico_range_high, 
			last_fico_range_low, 
	        collections_12_mths_ex_med, 
			mths_since_last_major_derog, 
	        acc_now_delinq, 
			tot_coll_amt, 
			tot_cur_bal, 
			total_rev_hi_lim, 
	        acc_open_past_24mths, 
			avg_cur_bal, 
			bc_open_to_buy, 
			bc_util, 
	        chargeoff_within_12_mths, 
			delinq_amnt, 
			mo_sin_old_il_acct, 
	        mo_sin_old_rev_tl_op, 
			mo_sin_rcnt_rev_tl_op, 
			mo_sin_rcnt_tl, 
	        mort_acc, 
			mths_since_recent_bc, 
			mths_since_recent_bc_dlq, 
	        mths_since_recent_inq, 
			mths_since_recent_revol_delinq, 
	        num_accts_ever_120_pd, 
			num_actv_bc_tl, 
			num_actv_rev_tl, 
	        num_bc_sats, 
			num_bc_tl, 
			num_il_tl, 
			num_op_rev_tl, 
			num_rev_accts, 
	        num_rev_tl_bal_gt_0, 
			num_sats, 
			num_tl_120dpd_2m, 
			num_tl_30dpd, 
	        num_tl_90g_dpd_24m, 
			num_tl_op_past_12m, 
			pct_tl_nvr_dlq, 
	        percent_bc_gt_75, 
			pub_rec_bankruptcies, 
			tax_liens, 
			tot_hi_cred_lim, 
	        total_bal_ex_mort, 
			total_bc_limit, 
			total_il_high_credit_limit, 
	        disbursement_method, 
			debt_settlement_flag, 
			debt_settlement_flag_date, 
	        settlement_status, 
			settlement_date, 
			settlement_amount, 
	        settlement_percentage, 
			settlement_term
	    )
	    SELECT 
	        NULLIF(loan_id, '')::NUMERIC::INT,
	        NULLIF(loan_amnt, '')::NUMERIC,
	        NULLIF(funded_amnt, '')::NUMERIC,
	        NULLIF(funded_amnt_inv, '')::NUMERIC,
	        TRIM(term),
	        TRIM(int_rate),
	        NULLIF(installment, '')::NUMERIC,
	        TRIM(grade),
	        TRIM(sub_grade),
	        TRIM(emp_length),
	        TRIM(home_ownership),
	        NULLIF(annual_inc, '')::NUMERIC,
	        TRIM(issue_d),
	        TRIM(loan_status),
	        TRIM(purpose),
	        TRIM(addr_state),
	        NULLIF(dti, '')::NUMERIC,
	        NULLIF(delinq_2yrs, '')::NUMERIC::INT,
	        TRIM(earliest_cr_line),
	        NULLIF(fico_range_low, '')::NUMERIC::INT,
	        NULLIF(fico_range_high, '')::NUMERIC::INT,
	        NULLIF(inq_last_6mths, '')::NUMERIC::INT,
	        NULLIF(mths_since_last_delinq, '')::NUMERIC::INT,
	        NULLIF(open_acc, '')::NUMERIC::INT,
	        NULLIF(pub_rec, '')::NUMERIC::INT,
	        NULLIF(revol_bal, '')::NUMERIC,
	        TRIM(revol_util),
	        NULLIF(total_acc, '')::NUMERIC::INT,
	        NULLIF(out_prncp, '')::NUMERIC,
	        NULLIF(out_prncp_inv, '')::NUMERIC,
	        NULLIF(total_pymnt, '')::NUMERIC,
	        NULLIF(total_rec_prncp, '')::NUMERIC,
	        NULLIF(total_rec_int, '')::NUMERIC,
	        NULLIF(total_rec_late_fee, '')::NUMERIC,
	        NULLIF(recoveries, '')::NUMERIC,
	        NULLIF(collection_recovery_fee, '')::NUMERIC,
	        TRIM(last_pymnt_d),
	        NULLIF(last_pymnt_amnt, '')::NUMERIC,
	        TRIM(last_credit_pull_d),
	        NULLIF(last_fico_range_high, '')::NUMERIC::INT,
	        NULLIF(last_fico_range_low, '')::NUMERIC::INT,
	        NULLIF(collections_12_mths_ex_med, '')::NUMERIC::INT,
	        NULLIF(mths_since_last_major_derog, '')::NUMERIC::INT,
	        NULLIF(acc_now_delinq, '')::NUMERIC::INT,
	        NULLIF(tot_coll_amt, '')::NUMERIC,
	        NULLIF(tot_cur_bal, '')::NUMERIC,
	        NULLIF(total_rev_hi_lim, '')::NUMERIC,
	        NULLIF(acc_open_past_24mths, '')::NUMERIC::INT,
	        NULLIF(avg_cur_bal, '')::NUMERIC,
	        NULLIF(bc_open_to_buy, '')::NUMERIC,
	        NULLIF(bc_util, '')::NUMERIC,
	        NULLIF(chargeoff_within_12_mths, '')::NUMERIC::INT,
	        NULLIF(delinq_amnt, '')::NUMERIC,
	        NULLIF(mo_sin_old_il_acct, '')::NUMERIC::INT,
	        NULLIF(mo_sin_old_rev_tl_op, '')::NUMERIC::INT,
	        NULLIF(mo_sin_rcnt_rev_tl_op, '')::NUMERIC::INT,
	        NULLIF(mo_sin_rcnt_tl, '')::NUMERIC::INT,
	        NULLIF(mort_acc, '')::NUMERIC::INT,
	        NULLIF(mths_since_recent_bc, '')::NUMERIC::INT,
	        NULLIF(mths_since_recent_bc_dlq, '')::NUMERIC::INT,
	        NULLIF(mths_since_recent_inq, '')::NUMERIC::INT,
	        NULLIF(mths_since_recent_revol_delinq, '')::NUMERIC::INT,
	        NULLIF(num_accts_ever_120_pd, '')::NUMERIC::INT,
	        NULLIF(num_actv_bc_tl, '')::NUMERIC::INT,
	        NULLIF(num_actv_rev_tl, '')::NUMERIC::INT,
	        NULLIF(num_bc_sats, '')::NUMERIC::INT,
	        NULLIF(num_bc_tl, '')::NUMERIC::INT,
	        NULLIF(num_il_tl, '')::NUMERIC::INT,
	        NULLIF(num_op_rev_tl, '')::NUMERIC::INT,
	        NULLIF(num_rev_accts, '')::NUMERIC::INT,
	        NULLIF(num_rev_tl_bal_gt_0, '')::NUMERIC::INT,
	        NULLIF(num_sats, '')::NUMERIC::INT,
	        NULLIF(num_tl_120dpd_2m, '')::NUMERIC::INT,
	        NULLIF(num_tl_30dpd, '')::NUMERIC::INT,
	        NULLIF(num_tl_90g_dpd_24m, '')::NUMERIC::INT,
	        NULLIF(num_tl_op_past_12m, '')::NUMERIC::INT,
	        NULLIF(pct_tl_nvr_dlq, '')::NUMERIC,
	        NULLIF(percent_bc_gt_75, '')::NUMERIC,
	        NULLIF(pub_rec_bankruptcies, '')::NUMERIC::INT,
	        NULLIF(tax_liens, '')::NUMERIC::INT,
	        NULLIF(tot_hi_cred_lim, '')::NUMERIC,
	        NULLIF(total_bal_ex_mort, '')::NUMERIC,
	        NULLIF(total_bc_limit, '')::NUMERIC,
	        NULLIF(total_il_high_credit_limit, '')::NUMERIC,
	        TRIM(disbursement_method),
	        TRIM(debt_settlement_flag),
	        TRIM(debt_settlement_flag_date),
	        TRIM(settlement_status),
	        TRIM(settlement_date),
	        NULLIF(settlement_amount, '')::NUMERIC,
	        NULLIF(settlement_percentage, '')::NUMERIC,
	        NULLIF(settlement_term, '')::NUMERIC::INT
	    FROM bronze.raw_loans
		WHERE loan_id ~ '^[0-9]+$';

EXCEPTION WHEN OTHERS THEN
	RAISE NOTICE '================================================';
    RAISE NOTICE 'ERROR OCCURRED DURING SILVER LOAD!';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Code: %', SQLSTATE;
    RAISE NOTICE '================================================';
END;

    v_batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer is Completed';
    RAISE NOTICE 'Total Load Duration: %', v_batch_end_time - v_batch_start_time;
    RAISE NOTICE '==========================================';
END;
$$;

CALL silver.load_silver();