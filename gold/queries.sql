select * from data_warehousing.gold.dim_customers;
select * from data_warehousing.gold.dim_products;
select * from data_warehousing.gold.fact_sales;

select sales.order_number, concat(customer.first_name, ' ', customer.last_name) as name, product.product_name, sales.price
  from data_warehousing.gold.fact_sales sales
  join data_warehousing.gold.dim_products product
      on sales.product_key = product.product_id
  join data_warehousing.gold.dim_customers customer
      on sales.customer_key=  customer.customer_id
order by sales.order_number;
