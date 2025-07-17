USE retail;

SELECT * FROM product; -- product_id, description, price, category, max_qty
SELECT * FROM member; -- member_id, first_name, last_name, store_id, reg_date
SELECT * FROM tran_hdr; -- tran_id, store_id, member_id, tran_dt
SELECT * FROM tran_dtl; -- tran_id, product_id, qty, amt, tran_dt

-- Total Sale per Member
WITH tran_mem_tbl AS 
	(SELECT th.tran_id, member_id, amt FROM tran_hdr th INNER JOIN tran_dtl td ON th.tran_id = td.tran_id)
SELECT member_id, ROUND(SUM(amt), 2) AS total_sale FROM tran_mem_tbl GROUP BY member_id ORDER BY total_sale DESC;

-- Total Average Sale per Member
WITH tran_mem_tbl AS 
	(SELECT th.tran_id, member_id, amt FROM tran_hdr th INNER JOIN tran_dtl td ON th.tran_id = td.tran_id), 
    tran_amts AS
    (SELECT tran_id, member_id, SUM(amt) AS tran_amt FROM tran_mem_tbl GROUP BY tran_id, member_id)
SELECT member_id, ROUND(AVG(tran_amt), 2) AS avg_sale, COUNT(DISTINCT tran_id) AS num_trans 
FROM tran_amts GROUP BY member_id ORDER BY avg_sale DESC;

-- Yearly Sale per Member
WITH tran_mem_tbl AS 
	(SELECT th.tran_id, YEAR(th.tran_dt) AS year, member_id, amt FROM tran_hdr th INNER JOIN tran_dtl td ON th.tran_id = td.tran_id)
SELECT member_id, year, SUM(amt) AS yearly_sales FROM tran_mem_tbl GROUP BY member_id, year ORDER BY member_id, year;