--1 случай
--Используем CTE с оконной функцией LAG() для получения значений из
--ab.account_out_sum за предыдущий день
WITH account_balance AS (
	SELECT
		ab.account_rk
		,ab.effective_date
		,ab.account_in_sum
		,ab.account_out_sum
		,LAG(ab.account_out_sum) OVER (PARTITION BY ab.account_rk ORDER BY ab.effective_date) AS prev_account_out_sum
	FROM
		rd.account_balance ab
--Используем CTE для поиска отличий account_in_sum от prev_account_out_sum.
--При совпадении значение не меняется
), corrected_balance AS (
	SELECT
		ab.account_rk
		,ab.effective_date
		,CASE 
			WHEN ab.account_in_sum != ab.prev_account_out_sum THEN ab.prev_account_out_sum 
			ELSE ab.account_in_sum 
		END AS corrected_account_in_sum
		,ab.account_out_sum
	FROM
		account_balance ab
)
SELECT 
	cb.account_rk
	,dc.currency_name
	,a.department_rk
	,cb.effective_date
	,cb.corrected_account_in_sum AS account_in_sum
	,cb.account_out_sum
FROM
	corrected_balance cb
JOIN rd.account a ON cb.account_rk = a.account_rk
JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
WHERE
	dc.effective_from_date <= cb.effective_date AND 
	dc.effective_to_date >= cb.effective_date
ORDER BY 
	cb.effective_date;



--2 случай
--Используем CTE с оконной функцией LAG() для получения значений из
--ab.account_out_sum за предыдущий день
WITH account_balance AS (
	SELECT
		ab.account_rk
		,ab.effective_date
		,ab.account_in_sum
		,ab.account_out_sum
		,LAG(ab.account_in_sum) OVER (PARTITION BY ab.account_rk ORDER BY ab.effective_date) AS prev_account_in_sum
	FROM
		rd.account_balance ab
--CTE для проверки различия account_out_sum от prev_account_in_sum.
--В случае неравенства происходит замена на account_in_sum текущего дня.
--Если равны, то оставляем account_out_sum
), corrected_balance AS (
	SELECT
		ab.account_rk
		,ab.effective_date
		,ab.account_in_sum
		,CASE 
			WHEN ab.account_out_sum <> ab.prev_account_in_sum THEN ab.account_in_sum 
			ELSE ab.account_out_sum 
		END AS corrected_account_out_sum
	FROM
		account_balance ab
)
SELECT
	cb.account_rk
	,dc.currency_name
	,a.department_rk
	,cb.effective_date
	,cb.account_in_sum
	,cb.corrected_account_out_sum AS account_out_sum
FROM
	corrected_balance cb
JOIN rd.account a ON cb.account_rk = a.account_rk
JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
WHERE
	dc.effective_from_date <= cb.effective_date AND 
	dc.effective_to_date >= cb.effective_date
ORDER BY 
	cb.effective_date;


--Обновляем значения в таблице rd.account_balance
WITH account_balance AS (
	SELECT
		ab.account_rk
		,ab.effective_date
		,ab.account_in_sum
		,ab.account_out_sum
		,LAG(ab.account_out_sum) OVER (PARTITION BY ab.account_rk ORDER BY ab.effective_date) AS prev_account_out_sum
	FROM
		rd.account_balance ab
)
UPDATE rd.account_balance ab
SET account_in_sum = b.prev_account_out_sum
FROM account_balance b
WHERE
    ab.account_rk = b.account_rk AND
    ab.effective_date = b.effective_date AND
    b.account_in_sum <> b.prev_account_out_sum;