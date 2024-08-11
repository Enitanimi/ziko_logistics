### Import necessary libraries

import pandas as pd
import os
import io
import dotenv
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient

# Extraction phase

ziko_df = pd.read_csv(r"ziko_logistics_data.csv")

# Transformation phase

ziko_df.fillna ({
    "Unit_Price" : ziko_df["Unit_Price"].mean(),
    "Total_Cost" : ziko_df["Total_Cost"].mean(),
    "Discount_Rate" : 0.0,
    "Return_Reason" : "Unknown"
}, inplace=True)

### Converting Date data type to datetime

ziko_df["Date"] = pd.to_datetime(ziko_df["Date"])

# Creating the Tables

# Customer Table

customer = ziko_df[[ "Customer_ID", "Customer_Name", "Customer_Phone", "Customer_Email", "Customer_Address"]].copy().drop_duplicates().reset_index(drop=True)

# Product Table

product = ziko_df[[ "Product_ID", "Product_List_Title","Quantity", "Unit_Price", "Discount_Rate"]].copy().drop_duplicates().reset_index(drop=True)

# Transaction_fact_table

transaction_fact = ziko_df.merge( customer, on = ["Customer_ID", "Customer_Name", "Customer_Phone", "Customer_Email", "Customer_Address"], how = "left" ) \
                            .merge( product, on = ["Product_ID", "Product_List_Title","Quantity", "Unit_Price", "Discount_Rate"], how = "left" ) \
                            [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID', 'Total_Cost', 'Sales_Channel','Order_Priority', 'Warehouse_Code', 'Ship_Mode', \
                            'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country' ]]


# Temporary Loading

customer.to_csv(r"Dataset/Customer.csv", index=False)
product.to_csv(r"Dataset/Product.csv", index=False)
transaction_fact.to_csv(r"Dataset/Transaction_Fact.csv", index=False)

print(" CSV file successfully loaded temporary into local machine")



load_dotenv()

# Setting up the Azure connection

connect_str = os.getenv("AzureCS")
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# validating the parameters that is needed to connect to the container
container_name = os.getenv("AzureCN")
container_client = blob_service_client.get_container_client(container_name)

# Create a function that would load the data into Azure blob storage as a Parquet file

def upload_df_to_blob_as_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type = "BlockBlob", overwrite = True)
    print (f"{blob_name} uploaded to Blob Storage successfully")

upload_df_to_blob_as_parquet(customer, container_client, "rawdata/customer.parquet")
upload_df_to_blob_as_parquet(product, container_client, "rawdata/product.parquet")
upload_df_to_blob_as_parquet(transaction_fact, container_client, "rawdata/transaction_fact.parquet")