# title: "Practicum II CS5200"
# author: "Yawen Chi"
# date: "Fall 2023"

# Load Libraries
library(DBI)
library(RSQLite)
library(XML)

# Connect to SQLite DB
dbcon <- dbConnect(RSQLite::SQLite(), dbname = "sqlDB")

# Drop tables if exists
dbExecute(dbcon, "DROP TABLE IF EXISTS sales")
dbExecute(dbcon, "DROP TABLE IF EXISTS products")
dbExecute(dbcon, "DROP TABLE IF EXISTS customers")
dbExecute(dbcon, "DROP TABLE IF EXISTS reps")

# Create reps table
dbExecute(dbcon, "
CREATE TABLE reps (
    rID TEXT PRIMARY KEY,
    first_name TEXT,
    sur_name TEXT,
    territory TEXT,
    commission REAL
)")

# Create customers table
dbExecute(dbcon, "
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    country TEXT
)")

# Create products table
dbExecute(dbcon, "
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
)")

# Create sales table
dbExecute(dbcon, "
CREATE TABLE sales (
    txnID TEXT PRIMARY KEY,
    date DATE,
    qty INTEGER,
    total NUMBER,
    currency TEXT,
    repID TEXT,
    customer_id INTEGER,
    product_id INTEGER,
    FOREIGN KEY(repID) REFERENCES reps(rID),
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(product_id) REFERENCES products(product_id)
)")

## Function that loads XML data into reps table
## Function that loads "pharmaReps-23.xml"
load_pharmaReps <- function(file_path, dbcon) {
  # Load the XML file
  reps_xml <- xmlParse(file_path)
  reps_nodes <- getNodeSet(reps_xml, "//rep")
  
  for (rep_node in reps_nodes) {
    rID <- xmlGetAttr(rep_node, "rID")
    first_name <- xmlValue(rep_node[["name"]][["first"]])
    sur_name <- xmlValue(rep_node[["name"]][["sur"]])
    territory <- xmlValue(rep_node[["territory"]])
    commission <- as.numeric(xmlValue(rep_node[["commission"]]))
    
    # Adjust rID to match format in sales table
    adjusted_rID <- sub("r", "", rID)
    
    # Insert data into the reps table
    reps_query <- sprintf("INSERT INTO reps (rID, first_name, sur_name, territory, commission) VALUES ('%s', '%s', '%s', '%s', %f)", adjusted_rID, first_name, sur_name, territory, commission)
    dbExecute(dbcon, reps_query)
  }
}

## Function that loads XML data into sales, customers and products table
## Function that loads "pharmaSalesTxn.xml"
load_pharmaSalesTxn <- function(file_path, dbcon) {
  txn_xml <- xmlParse(file_path)
  txn_nodes <- getNodeSet(txn_xml, "//txn")
  
  # Generate a unique identifier for the current file
  file_hash <- digest::digest(file_path)
  file_suffix <- substr(file_hash, 1, 8)  # Take first 8 characters of the hash
  
  for (txn_node in txn_nodes) {
    original_txnID <- xmlGetAttr(txn_node, "txnID")
    # Append the file suffix to the txnID to ensure uniqueness across files
    txnID <- paste0(original_txnID, "_", file_suffix)

    repID <- xmlGetAttr(txn_node, "repID")
    customer_name <- xmlValue(txn_node[["customer"]])
    country <- xmlValue(txn_node[["country"]])
    date <- format(as.Date(xmlValue(txn_node[["sale"]][["date"]]), "%m/%d/%Y"), "%Y-%m-%d")
    product_name <- xmlValue(txn_node[["sale"]][["product"]])
    qty <- as.integer(xmlValue(txn_node[["sale"]][["qty"]]))
    total <- as.numeric(xmlValue(txn_node[["sale"]][["total"]]))
    currency <- xmlGetAttr(txn_node[["sale"]][["total"]], "currency")
    
    # Check for existing customer
    customer_query <- sprintf("SELECT customer_id FROM customers WHERE name = '%s' AND country = '%s'", customer_name, country)
    customer_result <- dbGetQuery(dbcon, customer_query)
    if (nrow(customer_result) == 0) {
      insert_customer_query <- sprintf("INSERT INTO customers (name, country) VALUES ('%s', '%s')", customer_name, country)
      dbExecute(dbcon, insert_customer_query)
      customer_id <- dbGetQuery(dbcon, "SELECT last_insert_rowid()")$'last_insert_rowid()'
    } else {
      customer_id <- customer_result$customer_id[1]
    }
    
    # Check for existing product
    product_query <- sprintf("SELECT product_id FROM products WHERE name = '%s'", product_name)
    product_result <- dbGetQuery(dbcon, product_query)
    if (nrow(product_result) == 0) {
      insert_product_query <- sprintf("INSERT INTO products (name) VALUES ('%s')", product_name)
      dbExecute(dbcon, insert_product_query)
      product_id <- dbGetQuery(dbcon, "SELECT last_insert_rowid()")$'last_insert_rowid()'
    } else {
      product_id <- product_result$product_id[1]
    }
    
    # Insert data into the sales table
    sales_query <- sprintf("INSERT INTO sales (txnID, date, qty, total, currency, repID, customer_id, product_id) VALUES ('%s', '%s', %d, %f, '%s', '%s', %d, %d)", txnID, date, qty, total, currency, repID, customer_id, product_id)
    dbExecute(dbcon, sales_query)
  }
}

# Load XML from directory
# Get the current directory by running script to avoid absolute paths for XML files
script_dir <- dirname(rstudioapi::getActiveDocumentContext()$path)

# Set the working directory to the script's directory
setwd(script_dir)
xml_folder <- "txn-xml"

# List XML files using the relative path from the script's directory
xml_files <- list.files(path = xml_folder, pattern = "\\.xml$", full.names = TRUE)

for (file_path in xml_files) {
  if (grepl("pharmaReps", basename(file_path))) {
    load_pharmaReps(file_path, dbcon)
  } else if (grepl("pharmaSalesTxn", basename(file_path))) {
    load_pharmaSalesTxn(file_path, dbcon)
  }
}

## Test and display results
result1 <- dbGetQuery(dbcon, "SELECT * FROM reps")
result2 <- dbGetQuery(dbcon, "SELECT * FROM customers LIMIT 30")
result3 <- dbGetQuery(dbcon, "SELECT * FROM products LIMIT 30")
result4 <- dbGetQuery(dbcon, "SELECT * FROM sales LIMIT 50")


# Display the result
print(result1)
print(result2)
print(result3)
print(result4)

# Disconnect db
dbDisconnect(dbcon)
