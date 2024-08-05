/*
По датам эффективности effective_to_date все таблицы имеют одинаковые значения
2999-12-31, тогда как effective_from_date различается в таблице rd.product
*/

SELECT 
    MIN(effective_from_date) AS min_date
    ,MAX(effective_from_date) AS max_date
FROM 
    dm.loan_holiday_info;
--2023-01-01/2023-08-11
SELECT
	effective_from_date
FROM
	dm.loan_holiday_info
WHERE
	effective_from_date BETWEEN '2023-03-15' AND '2023-08-10';
--Совпадения есть


SELECT 
    MIN(effective_from_date) AS min_date
    ,MAX(effective_from_date) AS max_date
FROM 
    rd.deal_info;
--2023-01-01/2023-08-11
SELECT
	effective_from_date
FROM
	rd.deal_info
WHERE
	effective_from_date BETWEEN '2023-03-15' AND '2023-08-10';
--В таблице отсутствуют записи за этот интервал


SELECT 
    MIN(effective_from_date) AS min_date
    ,MAX(effective_from_date) AS max_date
FROM 
    rd.loan_holiday;
--2023-01-01/2023-08-11
SELECT
	effective_from_date
FROM
	rd.loan_holiday
WHERE
	effective_from_date BETWEEN '2023-03-15' AND '2023-08-10';
--Совпадения есть


SELECT 
    MIN(effective_from_date) AS min_date
    ,MAX(effective_from_date) AS max_date
FROM 
    rd.product;
--2023-03-15/2023-03-15
SELECT
	effective_from_date
FROM
	rd.product
WHERE
	effective_from_date BETWEEN '2023-03-15' AND '2023-08-10';
	--effective_from_date BETWEEN '2023-01-01' AND '2023-03-14';
--Данный фильтр вернет пустую таблицу, означающий отсутствие данных в период
--с 2023-01-01 по 2023-03-14


SELECT *
FROM 
	rd.product_new pn
LEFT JOIN rd.product p ON pn.product_rk = p.product_rk
WHERE 
	pn.effective_from_date = p.effective_from_date 
	AND pn.product_name != p.product_name
--Сравниваем актуальную выгрузку данных со старой таблицей product

SELECT *
FROM
	rd.deal_info_new din
LEFT JOIN rd.deal_info di ON din.deal_rk = di.deal_rk
WHERE 
	din.effective_from_date != di.effective_from_date
ORDER BY di.effective_from_date
--Сравниваем актуальную выгрузку данных со старой таблицей deal_info


--Создаем новую витрину dm.loan_holiday_info_new
CREATE TABLE IF NOT EXISTS dm.loan_holiday_info_new(
	deal_rk bigint NOT NULL
	,effective_from_date date NOT NULL
	,effective_to_date date NOT NULL
	,agreement_rk bigint
	,client_rk bigint
	,department_rk bigint
	,product_rk bigint
	,product_name text
	,deal_type_cd text
	,deal_start_date date
	,deal_name text
	,deal_number text
	,deal_sum numeric
	,loan_holiday_type_cd text
	,loan_holiday_start_date date
	,loan_holiday_finish_date date
	,loan_holiday_fact_finish_date date
	,loan_holiday_finish_flg boolean
	,loan_holiday_last_possible_date date
);


--Создаем процедуру для наполнения витрины dm.loan_holiday_info_new
CREATE OR REPLACE PROCEDURE dm.loan_holiday_info_new()
LANGUAGE plpgsql AS $$
BEGIN
	WITH deal AS (
		SELECT
			deal_rk
			,deal_num --Номер сделки
			,deal_name --Наименование сделки
			,deal_sum --Сумма сделки
			,client_rk --Ссылка на клиента
			,agreement_rk --Ссылка на договор
			,deal_start_date --Дата начала действия сделки
			,department_rk --Ссылка на отделение
			,product_rk -- Ссылка на продукт
			,deal_type_cd
			,effective_from_date
			,effective_to_date
		FROM
			rd.deal_info_new
		), loan_holiday AS (
		SELECT
			deal_rk
			,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
			,loan_holiday_start_date     --Дата начала кредитных каникул
			,loan_holiday_finish_date    --Дата окончания кредитных каникул
			,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
			,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
			,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
			,effective_from_date
			,effective_to_date
		FROM
			rd.loan_holiday
		), product AS (
		SELECT
			product_rk
			,product_name
			,effective_from_date
			,effective_to_date
		FROM
			rd.product_new
		), holiday_info AS (
		SELECT
			d.deal_rk
			,lh.effective_from_date
			,lh.effective_to_date
			,d.deal_num as deal_number --Номер сделки
			,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
			,lh.loan_holiday_start_date     --Дата начала кредитных каникул
			,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
			,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
			,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
			,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
			,d.deal_name --Наименование сделки
			,d.deal_sum --Сумма сделки
			,d.client_rk --Ссылка на контрагента
			,d.agreement_rk --Ссылка на договор
			,d.deal_start_date --Дата начала действия сделки
			,d.department_rk --Ссылка на ГО/филиал
			,d.product_rk -- Ссылка на продукт
			,p.product_name -- Наименование продукта
			,d.deal_type_cd -- Наименование типа сделки
		FROM
			deal d
		LEFT JOIN loan_holiday lh ON 1=1
								AND d.deal_rk = lh.deal_rk
								AND d.effective_from_date = lh.effective_from_date
		LEFT JOIN product p ON p.product_rk = d.product_rk
							AND p.effective_from_date = d.effective_from_date
	)
	INSERT INTO dm.loan_holiday_info_new (deal_rk
									,effective_from_date
									,effective_to_date
									,agreement_rk
									,client_rk
									,department_rk
									,product_rk
									,product_name
									,deal_type_cd
									,deal_start_date
									,deal_name
									,deal_number
									,deal_sum
									,loan_holiday_type_cd
									,loan_holiday_start_date
									,loan_holiday_finish_date
									,loan_holiday_fact_finish_date
									,loan_holiday_finish_flg
									,loan_holiday_last_possible_date)
	SELECT
		deal_rk
		,effective_from_date
		,effective_to_date
		,agreement_rk
		,client_rk
		,department_rk
		,product_rk
		,product_name
		,deal_type_cd
		,deal_start_date
		,deal_name
		,deal_number
		,deal_sum
		,loan_holiday_type_cd
		,loan_holiday_start_date
		,loan_holiday_finish_date
		,loan_holiday_fact_finish_date
		,loan_holiday_finish_flg
		,loan_holiday_last_possible_date
	FROM
		holiday_info;
END;
$$;

CALL dm.loan_holiday_info_new();

--Запрос на поиск совпадений со старой таблицей dm.loan_holiday_info
SELECT *
FROM 
	dm.loan_holiday_info_new lhn
JOIN dm.loan_holiday_info lh ON lhn.deal_rk = lh.deal_rk