# Summary. 

The Repository contains a PySpark AWS Glue ETL utility to add partition to glue when the input S3 Data Partition has special URL encoded format such as the one shown below

s3://bucket-name/nytaxi/partition_date=2022%2F10%2F04/

Partitioning data by date is a common theme at customers in most industries (FSI, Healthcare, Retail etc.) and some of the common date formats that are supported by on-prem data lakes such as Hadoop include dates of the format date=YYYY-MM-DD, date=YYYY/MM/DD. When it comes to handling date=YYYY/MM/DD, S3 doesn’t recognize these as valid prefixes and will be unable to create a partition folder this way and requires a special url encoding (using a %2F representation for “/” character). Also, Glue crawler cannot recognize these url encoded %2F characters. The purpose of the prescriptive guidance is to show how both S3 and Glue can be made to accommodate this pattern so that the partition values rightly show up as YYYY/MM/DD in glue tables.
