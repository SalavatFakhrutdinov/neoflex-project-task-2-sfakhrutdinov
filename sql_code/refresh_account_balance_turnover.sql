CREATE OR REPLACE PROCEDURE refresh_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
	--Перезагрузка данных в витрину dm.account_balance_turnover
	DELETE FROM dm.account_balance_turnover;
	
	INSERT INTO dm.account_balance_turnover (account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
	SELECT 
		ab.account_rk
		,COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name
		,a.department_rk
		,ab.effective_date
		,ab.account_in_sum
		,ab.account_out_sum
	FROM rd.account_balance ab
	LEFT JOIN rd.account a ON ab.account_rk = a.account_rk
	LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
	WHERE 
		dc.effective_from_date <= ab.effective_date AND 
		dc.effective_to_date >= ab.effective_date
	ORDER BY 
		ab.effective_date;
END;
$$;

CALL refresh_account_balance_turnover();