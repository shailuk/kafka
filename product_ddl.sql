/*
Step 1: MySQL Table Setup
Create a table named 'product' in MySQL database with the following
columns:
➔ id - INT (Primary Key)
➔ name - VARCHAR
➔ category - VARCHAR
➔ price - FLOAT
➔ last_updated - TIMESTAMP
*/

Use Test_Shailesh 
Go

CREATE TABLE product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    price FLOAT,
    last_updated DATETIME DEFAULT GETDATE()
);

select * from product

/*
Step 2 :
Create a table named 'product_tbl_last_fetch' with one column
that tracks the last fetch time of the product table.
*/

CREATE TABLE product_last_fetch (
last_fetch DATETIME DEFAULT GETDATE()
);


--select * from product where last_updated > '1900-01-01 00:00:00'
select * from test_shailesh..product_last_fetch

--Insert sample data into product table 
INSERT INTO product (product_id, product_name, product_category, price) VALUES
(1, 'Laptop', 'Electronics', 999.99),
(2, 'Smartphone', 'Electronics', 499.99),
(3, 'Table', 'Furniture', 199.99),
(4, 'Chair', 'Furniture', 89.99),
(5, 'Headphones', 'Electronics', 199.99),
(6, 'Monitor', 'Electronics', 349.99),
(7, 'Keyboard', 'Accessories', 79.99),
(8, 'Mouse', 'Accessories', 29.99),
(9, 'Desk Lamp', 'Lighting', 39.99),
(10, 'USB Cable', 'Accessories', 9.99);

--- Insert data into product_last_fetch
use Test_shailesh
Go
INSERT INTO product_last_fetch (last_fetch) VALUES
('1900-01-01 00:00:00');

Update product_last_fetch set last_fetch = '1900-01-01 00:00:00'

select * from product_last_fetch

Truncate TABLE product_last_fetch

