SELECT to_char(order_approved_at,'YYYY-MM') AS order_month,
		count(order_id) AS order_count
		FROM olist_orders
GROUP BY order_month;

SELECT to_char(order_approved_at,'YYYY-MM') AS order_month,
		order_status,
		count(order_id) AS order_count
		FROM olist_orders
GROUP BY order_month,order_status;

SELECT op.product_category_name,
		count(o.order_id) AS order_count
FROM olist_orders AS o
LEFT JOIN olist_order_items AS oi
	ON o.order_id=oi.order_id
LEFT JOIN olist_products AS op
	ON oi.product_id=op.product_id
	GROUP BY product_category_name
ORDER BY 2 DESC;


SELECT to_char(order_approved_at,'YYYY-MM') AS order_month,
		op.product_category_name,
		count(o.order_id) AS order_count
FROM olist_orders AS o
LEFT JOIN olist_order_items AS oi
	ON o.order_id=oi.order_id
LEFT JOIN olist_products AS op
	ON oi.product_id=op.product_id
	GROUP BY order_month, product_category_name
ORDER BY 3 DESC;


SELECT to_char(order_approved_at,'DAY') as order_day,
		count(order_id) as order_count
FROM olist_orders
GROUP BY order_day


SELECT extract (day from order_approved_at) as order_day,
		count(order_id) as order_count
FROM olist_orders
GROUP BY order_day


SELECT cc.customer_city,
       count(o.order_id) AS order_count
FROM olist_orders AS o
LEFT JOIN( SELECT oc.customer_id,
           oc.customer_city
    FROM( SELECT o.customer_id,
               c.customer_city,
               count(o.order_id) AS order_count
        FROM olist_orders AS o
        LEFT JOIN olist_customers AS c ON c.customer_id = o.customer_id
        GROUP BY o.customer_id, c.customer_city
    ) AS oc	  
    JOIN( SELECT o.customer_id,
          	ROW_NUMBER() OVER (
				PARTITION BY o.customer_id ORDER BY count(o.order_id) DESC
			) AS rn
        FROM olist_orders AS o
        LEFT JOIN olist_customers AS c ON c.customer_id = o.customer_id
        GROUP BY o.customer_id
    ) AS rn ON oc.customer_id = rn.customer_id AND oc.order_count = rn.rn		  
) AS cc ON o.customer_id = cc.customer_id
GROUP BY cc.customer_city
ORDER BY order_count DESC;


SELECT top5.seller_id,
       count(DISTINCT oi.order_id) AS order_count,
       AVG(r.review_score) AS avg_review_score,
       count(DISTINCT r.review_comment_message) AS total_review_message
FROM (
    SELECT seller_id
    FROM (
        SELECT oi.seller_id,
               EXTRACT(DAY FROM order_delivered_customer_date - order_purchase_timestamp) AS order_delivery_time
        FROM olist_orders AS o
        LEFT JOIN olist_order_items AS oi ON o.order_id = oi.order_id
    ) AS order_delivery_date
    GROUP BY seller_id
    ORDER BY AVG(order_delivery_time)
    LIMIT 5
) AS top5
LEFT JOIN olist_order_items AS oi ON oi.seller_id = top5.seller_id
LEFT JOIN olist_order_reviews AS r ON r.order_id = oi.order_id
GROUP BY top5.seller_id;


SELECT oi.seller_id,
		count(DISTINCT p.product_category_name) AS category_count,
		count(DISTINCT oi.order_id) AS order_count
	FROM olist_order_items AS oi
	LEFT JOIN olist_products AS p ON oi.product_id=p.product_id
GROUP BY 1
ORDER BY 2 DESC


SELECT payment_installments,
		c.customer_city,
		count(DISTINCT o.customer_id) AS customer_count
FROM olist_order_payments AS p
LEFT JOIN olist_orders AS o ON p.order_id=o.order_id
LEFT JOIN olist_customers AS c ON c.customer_id=o.customer_id
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC


SELECT p.payment_type,
		SUM (CASE WHEN o.order_status NOT IN('cancelled','unavailable') THEN p.payment_value END) AS succ_payment_value,
		count(DISTINCT CASE WHEN o.order_status NOT IN ('cancelled', 'unavailable') THEN o.order_id END) AS order_count
FROM olist_order_payments AS p
LEFT JOIN olist_orders AS o ON p.order_id=o.order_id
GROUP BY 1


SELECT p.payment_installments,
		product_category_name,
		count(DISTINCT o.order_id) AS order_count
FROM olist_orders AS o
LEFT JOIN olist_order_payments AS p ON p.order_id=o.order_id
LEFT JOIN olist_order_items AS oi ON oi.order_id= o.order_id
LEFT JOIN olist_products AS pro ON pro.product_id= oi.product_id
WHERE p.payment_installments=1
GROUP BY 1,2
ORDER BY 3 DESC, 1 DESC


SELECT p.payment_installments,
		product_category_name,
		count(DISTINCT o.order_id) AS order_count
FROM olist_orders AS o
LEFT JOIN olist_order_payments AS p ON p.order_id=o.order_id
LEFT JOIN olist_order_items AS oi ON oi.order_id= o.order_id
LEFT JOIN olist_products AS pro ON pro.product_id= oi.product_id
WHERE p.payment_installments>1
GROUP BY 1,2
ORDER BY 3 DESC, 1 DESC


WITH rfm_metrics AS (
    SELECT 
        customer_id,
        MAX(invoicedate) AS latest_purchase_date,
        COUNT(DISTINCT invoiceno) AS frequency,
        SUM(quantity * unitprice) AS monetary_value
    FROM rfm
    GROUP BY customer_id
),

rfm_scores AS (
    SELECT
        customer_id,
        EXTRACT(DAY FROM '2011-12-09' - latest_purchase_date) AS recency,
        frequency,
        monetary_value,
        NTILE(5) OVER (ORDER BY EXTRACT(DAY FROM '2011-12-09' - latest_purchase_date)) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ) AS f_score,
        NTILE(5) OVER (ORDER BY monetary_value ) AS m_score
    FROM rfm_metrics
),
merge_mon_fre_score AS (
	SELECT
		customer_id,
		r_score,
		f_score + m_score AS fremon_score
	FROM rfm_scores
),
final_rfm_score AS(
	SELECT
		customer_id,
		r_score,
		NTILE(5) OVER (ORDER BY fremon_score ) AS fm_score
	FROM merge_mon_fre_score
)

SELECT customer_id,
		r_score,
		fm_score,
		CONCAT(r_score,fm_score) AS rfm_segment
FROM final_rfm_score


