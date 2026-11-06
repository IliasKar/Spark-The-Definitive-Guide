import os, sys
import logging
import datetime
from datetime import datetime
from py4j.java_gateway import JavaObject  
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from typing import Optional
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    coalesce,
    concat_ws,
    current_timestamp,
    expr,
    from_utc_timestamp,
    lit,
    max,
    sum,
    col,
    sha2,
    substring,
    to_date,
    date_format,
    when,
    broadcast,
    trim,
    row_number,
    desc,
    collect_list
)

def load_landed_delta_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_filters: str,
    table_selected_fields: list[str],
) -> SparkDataFrame:
    
    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .filter(table_filters)
        .select(table_selected_fields)
    )

    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df

def load_landed_portfolio_table(
    spark: SparkSession,
    catalog_landed: str,
    table_name: str,
    table_filters: str,
    table_selected_fields: list[str],
) -> SparkDataFrame:
    
    df = (
        spark.read.table(f"{catalog_landed}.{table_name}")
        .filter(table_filters)
        .select(table_selected_fields)
    )

    for col_name, col_type in df.dtypes:
        if col_type == "string":
            df = df.withColumn(col_name, trim(df[col_name]))

    return df

class PLMDBInternal:
    
    def __init__(self, spark: SparkSession, catalog_landed: str, run_date: str):
        self.spark = spark
        self.catalog_landed = catalog_landed
        self.run_date = run_date
        self.date_filter = f"businessdate = {self.run_date!r}"

    def customerRequestReport(self) -> SparkDataFrame:

        # ============================== cepalplatform_customerrequest ====================================

        customer_request_extra_filters = None
        if customer_request_extra_filters is not None:
            customer_request_extra_filters.append(self.date_filter)
            customer_request_final_filters = " AND ".join(
                customer_request_extra_filters
                )
        else:
            customer_request_final_filters = self.date_filter

        customer_request_selected_filters = [
            "CustomerRequestID", 
            "CustomerRequestCDI",
            "CustomerRequestDateCreated",
            "CustomerRequestDateModified",
            "CustomerRequestDate", 
            "CustomerRequestCreatedByUser",
            "CustomerRequesCreatedByUserChannel",
            "CustomerRequestLockedByUserChannel", 
            "CustomerRequestLockedByUser",
            "isLocked",
            "CustomerRequestTypeID",
            "CustomerRequestStatusID",
            "CustomerRequestDateCreated",
            "BusinessDate"
            ]

        customer_request = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequest",
            customer_request_final_filters,
            customer_request_selected_filters,
            )

        #customer_request = customer_request.withColumn(
        #    "CustomerRequestDate",
        #    date_format(to_date("CustomerRequestDate", "d/M/yyyy"), "yyyy-MM-dd")
        #    )

        # ============================== cepalplatform_customerrequestcomment ====================================

        customer_request_comment_extra_filters = None
        if customer_request_comment_extra_filters is not None:
            customer_request_comment_extra_filters.append(self.date_filter)
            customer_request_comment_final_filters = " AND ".join(
                customer_request_comment_extra_filters
                )
        else:
            customer_request_comment_final_filters = self.date_filter

        customer_request_comment_selected_filters = [
            "CustomerRequestID",
            "CustomerRequestStatusID",
            "CustomerRequestCommentID",
            "CustomerRequestCommentText",
            "BusinessDate",
            ]

        customer_request_comment = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequestcomment",
            customer_request_comment_final_filters,
            customer_request_comment_selected_filters,
            )

        window_spec = Window.partitionBy(
            "CustomerRequestID",
            "CustomerRequestStatusID"
            ).orderBy(col("CustomerRequestCommentID").desc())

        customer_request_latest_comment = (
            customer_request_comment
            .withColumn("rnk", row_number().over(window_spec))
            .filter(col("rnk") == 1).drop("rnk")
        )

        # ============================== cepalplatform_rfdtcustomerrequesttype ====================================

        customer_request_type_extra_filters = None
        if customer_request_type_extra_filters is not None:
            customer_request_type_extra_filters.append(self.date_filter)
            customer_request_type_final_filters = " AND ".join(
                customer_request_type_extra_filters
                )
        else:
            customer_request_type_final_filters = self.date_filter

        customer_request_type_selected_filters = ["ID",  "Category", "BusinessDate"]
        customer_request_type = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_rfdtcustomerrequesttype",
            customer_request_type_final_filters,
            customer_request_type_selected_filters,
            )

        # ============================== cepalplatform_rfdtcustomerrequeststatus ====================================

        customer_request_status_extra_filters = None
        if customer_request_status_extra_filters is not None:
            customer_request_status_extra_filters.append(self.date_filter)
            customer_request_status_final_filters = " AND ".join(
                customer_request_status_extra_filters
                )
        else:
            customer_request_status_final_filters = self.date_filter

        customer_request_status_selected_filters = ["ID", "Status", "BusinessDate"]
        customer_request_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_rfdtcustomerrequeststatus",
            customer_request_status_final_filters,
            customer_request_status_selected_filters,
            )

        # ============================== cepalplatform_customerrequestloan ====================================

        customer_request_loan_extra_filters = None
        if customer_request_loan_extra_filters is not None:
            customer_request_loan_extra_filters.append(self.date_filter)
            customer_request_loan_final_filters = " AND ".join(
                customer_request_loan_extra_filters
                )
        else:
            customer_request_loan_final_filters = self.date_filter

        customer_request_loan_selected_filters = [
            "CustomerRequestID", 
            "CustomerRequestLoanIsSelected",
            "CustomerRequestLoanBalance",
            "CustomerRequestVendorLoanID",
            "BusinessDate",
            ]

        customer_request_loan = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequestloan",
            customer_request_loan_final_filters,
            customer_request_loan_selected_filters,
            )

        customer_request_total_loan_balance = (
            customer_request_loan
            .filter(col("CustomerRequestLoanIsSelected").cast("Int") == 1)
            .groupBy("CustomerRequestID")
            .agg(sum("CustomerRequestLoanBalance").alias("CustomerRequestLoanBalance"))
        )

        # ============================== joins ====================================

        customer_request_report_joins = (
            customer_request.join(
                customer_request_type,
                customer_request["CustomerRequestTypeID"] == customer_request_type["ID"] 
            )
            .join(
                customer_request_status,
                customer_request["CustomerRequestStatusID"] == customer_request_status["ID"] 
            )
            .join(
                customer_request_latest_comment,
                (customer_request["CustomerRequestID"] == customer_request_latest_comment["CustomerRequestID"])
                & (customer_request["CustomerRequestStatusID"] == customer_request_latest_comment["CustomerRequestStatusID"]),
                "left"
            )
            .join(
                customer_request_loan,
                (customer_request["CustomerRequestID"] == customer_request_loan["CustomerRequestID"])
                & (customer_request_loan["CustomerRequestLoanIsSelected"].cast("Int") == 1),
                "left"
            )
            .join(
                customer_request_total_loan_balance,
                customer_request["CustomerRequestID"] == customer_request_total_loan_balance["CustomerRequestID"],
                "left" 
            )
        )

        # ============================== select columns of interest ====================================

        customer_request_report_filtered_cols = (
            customer_request_report_joins.select(
                customer_request.CustomerRequestCDI,
                customer_request.CustomerRequestID,
                customer_request_loan.CustomerRequestVendorLoanID,
                customer_request.CustomerRequestDateCreated,
                customer_request.CustomerRequestDateModified,
                customer_request.CustomerRequestDate,
                customer_request.CustomerRequestCreatedByUser,
                customer_request.CustomerRequesCreatedByUserChannel,
                customer_request.CustomerRequestLockedByUserChannel,
                customer_request.CustomerRequestLockedByUser,
                customer_request.isLocked,
                customer_request_type.Category, 
                customer_request_status.Status,
                customer_request_comment.CustomerRequestCommentText,
                customer_request_loan.CustomerRequestLoanBalance,
                customer_request.BusinessDate,
            )
        )

        # ============================== Accounts column ====================================

        customer_request_report_with_accounts = (
            customer_request_report_filtered_cols.groupBy("CustomerRequestCDI", "CustomerRequestID")
            .agg(concat_ws(",", collect_list("CustomerRequestVendorLoanID")).alias("Accounts"))
        )

        window_spec = (
            Window.partitionBy("CustomerRequestCDI", "CustomerRequestID")
            .orderBy("CustomerRequestID")
            )

        customer_request_report_dedup = (
            customer_request_report_filtered_cols
            .withColumn("duplicate", row_number().over(window_spec))
            .filter(col("duplicate") == 1)
            .drop("duplicate")
        )

        customer_request_report_dt = (
            customer_request_report_dedup.alias("x")
            .join(
                customer_request_report_with_accounts.alias("y"),
                on=[
                    col("x.CustomerRequestCDI") == col("y.CustomerRequestCDI"),
                    col("x.CustomerRequestID") == col("y.CustomerRequestID")],
                how="left"
            )
            .select(
                col("x.*"),
                col("y.Accounts"),
            )
        )

        # ============================== Final columns ====================================

        customer_request_report_df = (
            customer_request_report_dt.select(
            customer_request_report_dt.CustomerRequestID.cast("Int"),
            customer_request_report_dt.CustomerRequestCDI.cast("String"),
            customer_request_report_dt.CustomerRequestDateCreated.cast("Timestamp"),
            customer_request_report_dt.CustomerRequestDateModified.cast("Timestamp"),
            customer_request_report_dt.CustomerRequestDate.cast("String"),
            customer_request_report_dt.CustomerRequestCreatedByUser.cast("String"),
            customer_request_report_dt.CustomerRequesCreatedByUserChannel.cast("String").alias("CustomerRequestCreatedByUserChannel"),
            customer_request_report_dt.CustomerRequestLockedByUserChannel.cast("String"),
            customer_request_report_dt.CustomerRequestLockedByUser.cast("String"),
            customer_request_report_dt.isLocked.cast("Boolean"),
            customer_request_report_dt.Category.cast("String"),
            customer_request_report_dt.Status.cast("String"),
            customer_request_report_dt.CustomerRequestCommentText.cast("String"),
            customer_request_report_dt.Accounts.cast("String"),
            customer_request_report_dt.CustomerRequestLoanBalance.cast("Decimal(19,4)"),
            customer_request_report_dt.BusinessDate.cast("Date"),
            )
        )

        return customer_request_report_df
    
    def customerRequest(self) -> SparkDataFrame:

        # ============================== cepalplatform_customerrequest ====================================

        customer_request_extra_filters = None
        if customer_request_extra_filters is not None:
            customer_request_extra_filters.append(self.date_filter)
            customer_request_final_filters = " AND ".join(
                customer_request_extra_filters
                )
        else:
            customer_request_final_filters = self.date_filter

        customer_request_selected_filters = [
                    "CustomerRequestID", 
                    "CustomerRequestTypeID",
                    "CustomerRequestStatusID", 
                    "CustomerRequestCDI",
                    "CustomerRequestDateModified", 
                    "CustomerRequestDate", 
                    "CustomerRequestLockedByUser", 
                    "isLocked",
                    "CustomerRequestLockedByUserChannel",
                    "BusinessDate",
                    ]

        customer_request = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequest",
            customer_request_final_filters,
            customer_request_selected_filters,
            )
        
        customer_request = customer_request.withColumn(
            "CustomerRequestDate",
            date_format(
                to_date("CustomerRequestDate", "d/M/yyyy"),
                 "yyyy-MM-dd"
                 )
            )

        # ============================== cepalplatform_rfdtcustomerrequesttype ====================================

        customer_request_type_extra_filters = None
        if customer_request_type_extra_filters is not None:
            customer_request_type_extra_filters.append(self.date_filter)
            customer_request_type_final_filters = " AND ".join(
                customer_request_type_extra_filters
                )
        else:
            customer_request_type_final_filters = self.date_filter

        customer_request_type_selected_filters = ["ID", "Category", "BusinessDate"]
        customer_request_type = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_rfdtcustomerrequesttype",
            customer_request_type_final_filters,
            customer_request_type_selected_filters,
            )

        # ============================== cepalplatform_customerrequestdata ====================================

        customer_request_data_extra_filters = None
        if customer_request_data_extra_filters is not None:
            customer_request_data_extra_filters.append(self.date_filter)
            customer_request_data_final_filters = " AND ".join(
                customer_request_data_extra_filters
                )
        else:
            customer_request_data_final_filters = self.date_filter

        customer_request_data_selected_filters = ["CustomerCDI", "BusinessDate"]
        customer_request_data = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequestdata",
            customer_request_data_final_filters,
            customer_request_data_selected_filters,
            )

        # ============================== cepalplatform_customerrequestcomment ====================================

        customer_request_comment_extra_filters = None
        if customer_request_comment_extra_filters is not None:
            customer_request_comment_extra_filters.append(self.date_filter)
            customer_request_comment_final_filters = " AND ".join(
                customer_request_comment_extra_filters
                )
        else:
            customer_request_comment_final_filters = self.date_filter

        customer_request_comment_selected_filters = [
            "CustomerRequestID",
            "CustomerRequestCommentID",
            "CustomerRequestStatusID",
            "CustomerRequestCommentText", 
            "CustomerRequestCreatedByUser", 
            "CustomerRequestCreatedByUserChannel",
            "CustomerRequestDateCreated",
            "BusinessDate"
            ]

        customer_request_comment = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequestcomment",
            customer_request_comment_final_filters,
            customer_request_comment_selected_filters,
            )

        # ============================== cepalplatform_rfdtcustomerrequeststatus ====================================

        customer_request_status_extra_filters = None
        if customer_request_status_extra_filters is not None:
            customer_request_status_extra_filters.append(self.date_filter)
            customer_request_status_final_filters = " AND ".join(
                customer_request_status_extra_filters
                )
        else:
            customer_request_status_final_filters = self.date_filter

        customer_request_status_selected_filters = ["ID", "Status", "BusinessDate"]
        customer_request_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_rfdtcustomerrequeststatus",
            customer_request_status_final_filters,
            customer_request_status_selected_filters,
            )

        # ============================== joins ====================================

        customer_request_joined = (
            customer_request.join(
                customer_request_type,  
                customer_request["CustomerRequestTypeID"] == customer_request_type["ID"]
            )
            .join(
                customer_request_data,
                customer_request["CustomerRequestCDI"] == customer_request_data["CustomerCDI"]
            )
            .join(
                customer_request_comment,  
                customer_request["CustomerRequestID"] == customer_request_comment["CustomerRequestID"]
            )
            .join(
                customer_request_status.alias("customer_request_status_a"),
                customer_request["CustomerRequestStatusID"] == col("customer_request_status_a.ID")
            )
            .join(
                customer_request_status.alias("customer_request_status_b"),
                customer_request_comment["CustomerRequestStatusID"] == col("customer_request_status_b.ID")
            )
        )

        # ============================== select final columns ====================================

        customer_request_df = (
            customer_request_joined.select(
                customer_request.CustomerRequestID.cast("Int"), 
                customer_request_data.CustomerCDI.cast("String"),  
                customer_request.CustomerRequestTypeID.cast("Int"),
                customer_request_type.Category.cast("String").alias("CustomerRequestType"), 
                customer_request.CustomerRequestStatusID.cast("Int"), 
                col("customer_request_status_a.Status").cast("String").alias("CustomerRequestStatus"), 
                customer_request.CustomerRequestDate.cast("Date"), 
                customer_request.CustomerRequestDateModified.cast("Timestamp"),
                customer_request_comment.CustomerRequestCommentID.cast("Int"),
                customer_request_comment.CustomerRequestStatusID.cast("Int").alias("CustomerRequestCommentStatusID"),
                col("customer_request_status_b.Status").cast("String").alias("CustomerRequestCommentStatus"),
                customer_request_comment.CustomerRequestCommentText.cast("String"), 
                customer_request_comment.CustomerRequestCreatedByUser.cast("String"), 
                customer_request_comment.CustomerRequestCreatedByUserChannel.cast("String"),
                customer_request_comment.CustomerRequestDateCreated.cast("Timestamp"), 
                customer_request.isLocked.cast("Int"),
                customer_request.CustomerRequestLockedByUser.cast("String"), 
                customer_request.CustomerRequestLockedByUserChannel.cast("String"),
                customer_request.BusinessDate.cast("Date"),
            )
        )

        return customer_request_df
    
    def customerRequestLoan(self) -> SparkDataFrame:

        # ============================== cepalplatform_customerrequest ====================================

        customer_request_extra_filters = None
        if customer_request_extra_filters is not None:
            customer_request_extra_filters.append(self.date_filter)
            customer_request_final_filters = " AND ".join(
                customer_request_extra_filters
                )
        else:
            customer_request_final_filters = self.date_filter

        customer_request_selected_filters = ["CustomerRequestID",  "BusinessDate"]
        customer_request = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequest",
            customer_request_final_filters,
            customer_request_selected_filters,
            )

        # ============================== cepalplatform_customerrequestloan ====================================

        customer_request_loan_extra_filters = None
        if customer_request_loan_extra_filters is not None:
            customer_request_loan_extra_filters.append(self.date_filter)
            customer_request_loan_final_filters = " AND ".join(
                customer_request_loan_extra_filters
                )
        else:
            customer_request_loan_final_filters = self.date_filter

        customer_request_loan_selected_filters = [
            "CustomerRequestID",
            "CustomerRequestLoanID",
            "CustomerRequestVendorLoanID",
            "CustomerRequestLoanBalance",
            "CustomerRequestLoanIsSelected",
            "BusinessDate",
            ]

        customer_request_loan = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_customerrequestloan",
            customer_request_loan_final_filters,
            customer_request_loan_selected_filters,
            )

        # ============================== joins ====================================

        customer_request_loan_joined = (
            customer_request.join(
                customer_request_loan,
                customer_request["CustomerRequestID"] == customer_request_loan["CustomerRequestID"]
                )
            )

        # ============================== select final columns ====================================

        customer_request_loan_df = (
            customer_request_loan_joined.select(
                customer_request_loan.CustomerRequestLoanID.cast("Int"),
                customer_request.CustomerRequestID.cast("Int"),
                customer_request_loan.CustomerRequestVendorLoanID.cast("String"),
                customer_request_loan.CustomerRequestLoanBalance.cast("Decimal(19,4)"),
                customer_request_loan.CustomerRequestLoanIsSelected.cast("Boolean"),
                customer_request.BusinessDate.cast("Date"),
            )
        )

        return customer_request_loan_df