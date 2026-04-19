-- view for customer infomations

select * from silver.crm_cust_info AS ci;
select * from silver.erp_cust_az12 AS ca;
select * from silver.erp_loc_a101 AS cl;

CREATE VIEW gold.dim_customers AS
select
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntey AS country,
	ci.cst_marital_status AS marital_status,
	case when ci.cst_gender != 'N/A' THEN ci.cst_gender
		 ELSE COALESCE(ca.gen,'N/A')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
from silver.crm_cust_info AS ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as cl
on ci.cst_key = cl.cid;

SELECT * FROM gold.dim_customers;

-- view for product informations

SELECT * from silver.crm_prd_info
select * from silver.erp_px_cat_g1v2

CREATE VIEW gold.dim_products AS
select 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.MAINTENANCE AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
from silver.crm_prd_info AS pn
left join 
silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null

SELECT * FROM gold.dim_products;


--view for fact sales

CREATE VIEW gold.fact_sales AS 
select 
	sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_sales AS sales_amount,
	sls_quantity AS quantity,
	sls_price AS price
from silver.crm_sales_details AS sd
left join gold.dim_products AS pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers AS cu
on sd.sls_cust_id = cu.customer_id

select * from gold.fact_sales
