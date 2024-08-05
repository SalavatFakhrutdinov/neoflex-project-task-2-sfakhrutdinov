DELETE FROM dm.client WHERE client_rk IN (
	WITH duplicated_records AS (
		SELECT
			client_rk
			,effective_from_date
			,ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY client_rk) AS row_num
		FROM
			dm.client
	)
	SELECT
		client_rk
	FROM 
		duplicated_records
	WHERE
		row_num > 1
)