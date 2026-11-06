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
    when,
    broadcast,
    trim,
    row_number,
    desc
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

class LMSInternal:
    
    def __init__(self, spark: SparkSession, catalog_landed: str, run_date: str):
        self.spark = spark
        self.catalog_landed = catalog_landed
        self.run_date = run_date
        self.date_filter = f"businessdate = {self.run_date!r}"

    def loanSales(self) -> SparkDataFrame:

        # ============================== cepalplatform_lmsloansales ====================================

        loan_sales_extra_filters = None
        if loan_sales_extra_filters is not None:
            loan_sales_extra_filters.append(self.date_filter)
            loan_sales_final_filters = " AND ".join(
                loan_sales_extra_filters
            )
        else:
            loan_sales_final_filters = self.date_filter

        loan_sales_selected_filters = [
            "ID",
            "InvestorFK",
            "Description",
            "Status",
            "CepalManagement",
            "CreationDate",
            "CreationUser",
            "LastModifiedDate",
            "LastModifiedUser",
            "IsDeleted",
            "BusinessDate",
            ]

        loan_sales = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsloansales",
            loan_sales_final_filters,
            loan_sales_selected_filters,
            )
        
        # ============================== cepalplatform_lmsrfdtsalestatus ====================================

        sale_status_extra_filters = None
        if sale_status_extra_filters is not None:
            sale_status_extra_filters.append(self.date_filter)
            sale_status_final_filters = " AND ".join(
                sale_status_extra_filters
            )
        else:
            sale_status_final_filters = self.date_filter

        sale_status_selected_filters = ["ID", "Label", "BusinessDate"]
        sale_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsrfdtsalestatus",
            sale_status_final_filters,
            sale_status_selected_filters,
            )
        
        # ============================== joins ====================================

        lms_loan_sales = loan_sales.join(sale_status, loan_sales.Status == sale_status.ID)

        # ================== lms_loan_sales table schema ==================

        lms_loan_sales_target = lms_loan_sales.select(
            loan_sales.ID.cast("Int"),
            loan_sales.InvestorFK.cast("Int"),
            loan_sales.Description.cast("String"),
            loan_sales.Status.cast("Int"),
            sale_status.Label.cast("String").alias("Status_Description"),
            loan_sales.CepalManagement.cast("Boolean"),
            loan_sales.CreationDate.cast("Timestamp"),
            loan_sales.CreationUser.cast("String"),
            loan_sales.LastModifiedDate.cast("Timestamp"),
            loan_sales.LastModifiedUser.cast("String"),
            loan_sales.IsDeleted.cast("Boolean"),
            loan_sales.BusinessDate.cast("Date"),
            )

        return lms_loan_sales_target
    
    def loanSalesDetails(self) -> SparkDataFrame:

        # ============================== cepalplatform_lmsloansalesdetails ====================================

        loan_sales_details_extra_filters = None
        if loan_sales_details_extra_filters is not None:
            loan_sales_details_extra_filters.append(self.date_filter)
            loan_sales_details_final_filters = " AND ".join(
                loan_sales_details_extra_filters
            )
        else:
            loan_sales_details_final_filters = self.date_filter

        loan_sales_details_selected_filters = [
            "ID",
            "LoanSalesFK",
            "Portfolio",
            "AccountNumber",
            "EquivalentAmount",
            "EquivalentAmountByUser",
            "IsDeleted",
            "BusinessDate",
            ]

        loan_sales_details = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsloansalesdetails",
            loan_sales_details_final_filters,
            loan_sales_details_selected_filters,
            )

        loan_sales_details_filtered = loan_sales_details.filter(~coalesce(col("IsDeleted"), lit(False)))

        # ================== lms_loan_sales_details table schema ==================

        lms_loan_sales_details_target = loan_sales_details_filtered.select(
            loan_sales_details_filtered.ID.cast("Int"),
            loan_sales_details_filtered.LoanSalesFK.cast("Int"),
            loan_sales_details_filtered.Portfolio.cast("Int"),
            loan_sales_details_filtered.AccountNumber.cast("String"),
            loan_sales_details_filtered.EquivalentAmount.cast("decimal(19,4)"),
            loan_sales_details_filtered.EquivalentAmountByUser.cast("decimal(19,4)"),
            loan_sales_details_filtered.BusinessDate.cast("Date"),
        )

        return lms_loan_sales_details_target
    
    def investor(self) -> SparkDataFrame:

        # ============================== cepalplatform_lmsinvestor ====================================

        investor_extra_filters = None
        if investor_extra_filters is not None:
            investor_extra_filters.append(self.date_filter)
            investor_final_filters = " AND ".join(
                investor_extra_filters
            )
        else:
            investor_final_filters = self.date_filter

        investor_selected_filters = [
            "ID",
            "InvestorName",
            "InvestorAFM",
            "CreationDate",
            "CreationUser",
            "LastModifiedDate",
            "LastModifiedUser",
            "BusinessDate",
            ]

        investor = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsinvestor",
            investor_final_filters,
            investor_selected_filters,
            )

        # ================== lms_investor_target table schema ==================

        lms_investor_target = investor.select(
            investor.ID.cast("Int"),
            investor.InvestorName.cast("String"),
            investor.InvestorAFM.cast("String"),
            investor.CreationDate.cast("Timestamp"),
            investor.CreationUser.cast("String"),
            investor.LastModifiedDate.cast("Timestamp"),
            investor.LastModifiedUser.cast("String"),
            investor.BusinessDate.cast("Date"),
        )

        return lms_investor_target

    def loanSalesPayment(self) -> SparkDataFrame:

        # ============================== cepalplatform_lmsloansalespayment ====================================

        loan_sales_payment_extra_filters = None
        if loan_sales_payment_extra_filters is not None:
            loan_sales_payment_extra_filters.append(self.date_filter)
            loan_sales_payment_final_filters = " AND ".join(
                loan_sales_payment_extra_filters
            )
        else:
            loan_sales_payment_final_filters = self.date_filter

        loan_sales_payment_selected_filters = [
            "ID",
            "LoanSalesFK",
            "Type",
            "Amount",
            "CreationDate",
            "CreationUser",
            "PaymentDate",
            "FinalSaleDate",
            "TUN",
            "IsDeleted",
            "BusinessDate",
            ]

        loan_sales_payment = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsloansalespayment",
            loan_sales_payment_final_filters,
            loan_sales_payment_selected_filters,
            )
        
        loan_sales_payment_filtered = loan_sales_payment.filter(~coalesce(col("IsDeleted"), lit(False)))

        # ============================== cepalplatform_lmsrfdtpaymenttype ====================================

        payment_type_extra_filters = None
        if payment_type_extra_filters is not None:
            payment_type_extra_filters.append(self.date_filter)
            payment_type_final_filters = " AND ".join(
                payment_type_extra_filters
            )
        else:
            payment_type_final_filters = self.date_filter

        payment_type_selected_filters = [
            "ID",
            "Label",
            "BusinessDate",
            ]

        payment_type = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_lmsrfdtpaymenttype",
            payment_type_final_filters,
            payment_type_selected_filters,
            )
        # ============================== joins ====================================
        
        lms_loan_sales_payments = loan_sales_payment_filtered.join(payment_type, loan_sales_payment_filtered.Type == payment_type.ID)
        lms_loan_sales_payments_target = lms_loan_sales_payments.select(
            loan_sales_payment.ID.cast("Int"),
            loan_sales_payment.LoanSalesFK.cast("Int"),
            loan_sales_payment.Type.cast("Int"),
            payment_type.Label.cast("String").alias("TypeDescription"),
            loan_sales_payment.Amount.cast("decimal(19,4)"),
            loan_sales_payment.CreationDate.cast("Timestamp"),
            loan_sales_payment.CreationUser.cast("String"),
            loan_sales_payment.PaymentDate.cast("Timestamp"),
            loan_sales_payment.FinalSaleDate.cast("Timestamp"),
            loan_sales_payment.TUN.cast("String"),
            loan_sales_payment.BusinessDate.cast("Date"),
        )
        return lms_loan_sales_payments_target