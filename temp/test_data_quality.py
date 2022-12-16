# Databricks notebook source
from pyspark.sql import Row
assert Row(tableName="customers_raw_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customers_raw_temp").dtypes ==  [('customer_id', 'string'),
 ('tax_id', 'string'),
 ('tax_code', 'string'),
 ('customer_name', 'string'),
 ('state', 'string'),
 ('city', 'string'),
 ('postcode', 'string'),
 ('street', 'string'),
 ('number', 'string'),
 ('unit', 'string'),
 ('region', 'string'),
 ('district', 'string'),
 ('lon', 'string'),
 ('lat', 'string'),
 ('ship_to_address', 'string'),
 ('valid_from', 'string'),
 ('valid_to', 'string'),
 ('units_purchased', 'string'),
 ('loyalty_segment', 'string'),
 ('_rescued_data', 'string')], "Incorrect Schema"
