USE retail;

SELECT * FROM product; -- product_id, description, price, category, max_qty
SELECT * FROM member; -- member_id, first_name, last_name, store_id, reg_date
SELECT * FROM tran_hdr; -- tran_id, store_id, member_id, tran_dt
SELECT * FROM tran_dtl; -- tran_id, product_id, qty, amt, tran_dt

-- Total Sale per Product
SELECT p.product_id, description, ROUND(SUM(amt), 2) AS total_sale, SUM(qty) AS total_qty_sold
FROM product p INNER JOIN tran_dtl t ON p.product_id = t.product_id
GROUP BY p.product_id ORDER BY total_sale DESC;

-- Total Average Sale per Product
SELECT p.product_id, description, ROUND(AVG(amt), 2) AS avg_sale, ROUND(AVG(qty), 2) AS avg_qty_sold
FROM product p INNER JOIN tran_dtl t ON p.product_id = t.product_id
GROUP BY p.product_id ORDER BY avg_sale DESC;

-- Total