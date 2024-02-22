# title: "Practicum II CS5200"
# author: "Yawen Chi"
# date: "Fall 2023"

# Load Libraries
library(RMySQL)
library(DBI)
library(RSQLite)
library(XML)

# Info for connecting to remote database
db_name_fh <- "sql12667348"
db_user_fh <- "sql12667348"
db_host_fh <- "sql12.freemysqlhosting.net"
db_pwd_fh <- "tGigDY6BtE"
db_port_fh <- 3306

# Connect to MySQL server database
mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,
                      dbname = db_name_fh, host = db_host_fh, port = db_port_fh)
mydb <- mydb.fh

# Connect to SQLite database
dbcon <- dbConnect(RSQLite::SQLite(), dbname = "sqlDB")

# Drop tables if exists
dbExecute(mydb, "DROP TABLE IF EXISTS sales_facts")
dbExecute(mydb, "DROP TABLE IF EXISTS rep_facts")

# Create sales_facts table
sales_facts_sql <- "
CREATE TABLE sales_facts (
    sales_fact_id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(255),
    year TEXT,
    quarter TEXT,
    total_sales INT,
    total_units INT
);"

# Execute the query to create the sales_facts table
dbExecute(mydb, sales_facts_sql)

# Create rep_facts table
rep_facts_sql <- "
CREATE TABLE rep_facts (
    rep_fact_id INT AUTO_INCREMENT PRIMARY KEY,
    rep_name VARCHAR(255),
    product_name VARCHAR(255),
    year TEXT,
    quarter TEXT,
    total_sales INT,
    total_units INT
);"

# Execute the query to create the rep_facts table
dbExecute(mydb, rep_facts_sql)

# Load data from sqlite db into sales_facts_data
query_sales_facts <- "
SELECT 
    r.territory AS region,
    strftime('%Y', s.date) AS year,
    CASE 
        WHEN strftime('%m', s.date) IN ('01', '02', '03') THEN 'Q1'
        WHEN strftime('%m', s.date) IN ('04', '05', '06') THEN 'Q2'
        WHEN strftime('%m', s.date) IN ('07', '08', '09') THEN 'Q3'
        ELSE 'Q4'
    END AS quarter,
    SUM(s.total) AS total_sales,
    SUM(s.qty) AS total_units
FROM 
    sales s
JOIN 
    reps r ON s.repID = r.rID
GROUP BY 
    region, year, quarter
"

sales_facts_data <- dbGetQuery(dbcon, query_sales_facts)
# Load data into sales_facts in MySQL
dbWriteTable(mydb, "sales_facts", sales_facts_data, append = TRUE, row.names = FALSE, overwrite = FALSE)

# Load data from sqlite db into reps_facts_data
query_rep_facts <- "
SELECT 
    r.first_name || ' ' || r.sur_name AS rep_name,
    p.name AS product_name,
    strftime('%Y', s.date) AS year,
    CASE 
        WHEN strftime('%m', s.date) IN ('01', '02', '03') THEN 'Q1'
        WHEN strftime('%m', s.date) IN ('04', '05', '06') THEN 'Q2'
        WHEN strftime('%m', s.date) IN ('07', '08', '09') THEN 'Q3'
        ELSE 'Q4'
    END AS quarter,
    SUM(s.total) AS total_sales,
    SUM(s.qty) AS total_units
FROM 
    sales s
JOIN 
    reps r ON s.repID = r.rID
JOIN 
    products p ON s.product_id = p.product_id
GROUP BY 
    rep_name, product_name, year, quarter
"

rep_facts_data <- dbGetQuery(dbcon, query_rep_facts)
# Load data into rep_facts table
dbWriteTable(mydb, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)

# Show results for checking
result1 <- dbGetQuery(mydb, "SELECT * FROM sales_facts LIMIT 50")
result2 <- dbGetQuery(mydb, "SELECT * FROM rep_facts LIMIT 50")
print(result1)
print(result2)

# Disconnecting from dbs
dbDisconnect(mydb)
dbDisconnect(dbcon)