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

class ApplicationSMEInternal:
    
    def __init__(self, spark: SparkSession, catalog_landed: str, run_date: str):
        self.spark = spark
        self.catalog_landed = catalog_landed
        self.run_date = run_date
        self.date_filter = f"businessdate = {self.run_date!r}"

    def businessPlanCorpBankruptcy(self) -> SparkDataFrame:

        # ============================== cepalplatform_smevbusinessplandatacorpbankruptsy ====================================

        business_plan_corp_bankruptcy_extra_filters = None
        if business_plan_corp_bankruptcy_extra_filters is not None:
            business_plan_corp_bankruptcy_extra_filters.append(self.date_filter)
            business_plan_corp_bankruptcy_final_filters = " AND ".join(
                business_plan_corp_bankruptcy_extra_filters
            )
        else:
            business_plan_corp_bankruptcy_final_filters = self.date_filter

        business_plan_corp_bankruptcy_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "CollectionPlan_Month",
            "CollectionPlan_Amount",
            "PortfolioID",
            "BusinessDate",
            ]

        # Keys of the table: **BusinessPlanID, AgreementNumber, BusinessPlan_Status, BusinessPlanStrategy_StrategyType, BusinessPlanStrategy_Code, CollectionPlan_Month**
        business_plan_corp_bankruptcy = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandatacorpbankruptsy",
            business_plan_corp_bankruptcy_final_filters,
            business_plan_corp_bankruptcy_selected_filters,
            )

        agg_business_plan_corp_bankruptcy = (
            business_plan_corp_bankruptcy.groupBy(
                "BusinessPlan_ID",
                "AgreementNumber",
                "BusinessPlan_Status",
                "BusinessPlanStrategy_StrategyType",
                "BusinessPlanStrategy_Code",
                "CollectionPlan_Month"
            )
            .agg(sum("CollectionPlan_Amount").alias("CollectionPlan_Total_Amount"))
        )

        window_spec = Window.partitionBy(
            "BusinessPlan_ID",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Code",
            "CollectionPlan_Month"
            ).orderBy("CollectionPlan_Amount")

        dedup_business_plan_corp_bankruptcy = (
            business_plan_corp_bankruptcy.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
            )

        business_plan_corp_bankruptcy_join_condition = (
            (col("dedup.BusinessPlan_ID") == col("agg.BusinessPlan_ID"))
                &(col("dedup.AgreementNumber") == col("agg.AgreementNumber"))
                &(col("dedup.BusinessPlan_Status") == col("agg.BusinessPlan_Status"))
                &(col("dedup.BusinessPlanStrategy_StrategyType") == col("agg.BusinessPlanStrategy_StrategyType"))
                &(col("dedup.BusinessPlanStrategy_Code") == col("agg.BusinessPlanStrategy_Code"))
                &(col("dedup.CollectionPlan_Month") == col("agg.CollectionPlan_Month"))
        )

        business_plan_corp_bankruptcy_df = (
            dedup_business_plan_corp_bankruptcy.alias('dedup').join(
                agg_business_plan_corp_bankruptcy.alias("agg"),
                business_plan_corp_bankruptcy_join_condition,
                how="left"
            ).select(
                "dedup.*",
                "agg.CollectionPlan_Total_Amount"
                )
        )

        # ================== business_plan_corp_bankruptcy table schema ==================

        business_plan_corp_bankruptcy = business_plan_corp_bankruptcy_df.select(
            business_plan_corp_bankruptcy_df.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            business_plan_corp_bankruptcy_df.DebtorCDI.cast("String"),
            business_plan_corp_bankruptcy_df.AgreementNumber.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlan_Status.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            business_plan_corp_bankruptcy_df.BusinessPlan_SolutionType.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlan_Comments.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlanStrategy_Status.cast("String"),
            business_plan_corp_bankruptcy_df.Confidence.cast("String"),
            business_plan_corp_bankruptcy_df.ProposedBy.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlanStrategy_StrategyType.cast("String"),
            business_plan_corp_bankruptcy_df.BusinessPlanStrategy_Comments.cast("String"),
            business_plan_corp_bankruptcy_df.LastModifiedDate.cast("Date"),
            business_plan_corp_bankruptcy_df.CollectionPlan_Month.cast("Date"),
            business_plan_corp_bankruptcy_df.CollectionPlan_Total_Amount.alias("CollectionPlan_Amount").cast("Decimal(19, 2)"), # Important
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            lit(None).cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            business_plan_corp_bankruptcy_df.PortfolioID.cast("Int"),
            business_plan_corp_bankruptcy_df.BusinessDate.cast("Date"),
            lit("Business_Plan_Corp_Bankruptcy").cast("String").alias("tbl"),
            )

        return business_plan_corp_bankruptcy
    
    def businessPlanToolForc(self) -> SparkDataFrame:

        # ============================== cepalplatform_smevbusinessplandataforccbsnonre ====================================

        nonre_forc_extra_filters = None
        if nonre_forc_extra_filters is not None:
            nonre_forc_extra_filters.append(self.date_filter)
            nonre_forc_final_filters = " AND ".join(
                nonre_forc_extra_filters
            )
        else:
            nonre_forc_final_filters = self.date_filter

        nonre_forc_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "TimeToRecovery",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        nonre_forc = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandataforccbsnonre",
            nonre_forc_final_filters,
            nonre_forc_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandataforccbsre3rdparty =======================================

        re_3rdparty_forc_extra_filters = None
        if re_3rdparty_forc_extra_filters is not None:
            re_3rdparty_forc_extra_filters.append(self.date_filter)
            re_3rdparty_forc_final_filters = " AND ".join(
                re_3rdparty_forc_extra_filters
            )
        else:
            re_3rdparty_forc_final_filters = self.date_filter

        re_3rdparty_forc_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "AuctionPrice",
            "TimeToAuction",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        re_3rdparty_forc = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandataforccbsre3rdparty",
            re_3rdparty_forc_final_filters,
            re_3rdparty_forc_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandataforccbsreohold =======================================

        reo_hold_forc_extra_filters = None
        if reo_hold_forc_extra_filters is not None:
            reo_hold_forc_extra_filters.append(self.date_filter)
            reo_hold_forc_final_filters = " AND ".join(
                reo_hold_forc_extra_filters
            )
        else:
            reo_hold_forc_final_filters = self.date_filter

        reo_hold_forc_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "AuctionPrice",
            "TimeToAuction",
            "TimeToSell",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        reo_hold_forc = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandataforccbsreohold",
            reo_hold_forc_final_filters,
            reo_hold_forc_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandataforccbsreosale =======================================

        reo_sale_forc_extra_filters = None
        if reo_sale_forc_extra_filters is not None:
            reo_sale_forc_extra_filters.append(self.date_filter)
            reo_sale_forc_final_filters = " AND ".join(
                reo_sale_forc_extra_filters
            )
        else:
            reo_sale_forc_final_filters = self.date_filter

        reo_sale_forc_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "AuctionPrice",
            "TimeToAuction",
            "TimeToSell",
            "NetRecoveryAmount",
            "BusinessDate",
            # "TimeToAuctionRM"
        ]

        reo_sale_forc = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandataforccbsreosale",
            reo_sale_forc_final_filters,
            reo_sale_forc_selected_filters,
            )

        # ================== nonre_forc table schema ==================

        target_nonre_forc = nonre_forc.select(
            nonre_forc.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            nonre_forc.DebtorCDI.cast("String"),
            nonre_forc.AgreementNumber.cast("String"),
            nonre_forc.BusinessPlan_Status.cast("String"),
            nonre_forc.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            nonre_forc.BusinessPlan_SolutionType.cast("String"),
            nonre_forc.BusinessPlan_Comments.cast("String"),
            nonre_forc.BusinessPlanStrategy_Status.cast("String"),
            nonre_forc.Confidence.cast("String"),
            nonre_forc.ProposedBy.cast("String"),
            nonre_forc.BusinessPlanStrategy_StrategyType.cast("String"),
            nonre_forc.BusinessPlanStrategy_Comments.cast("String"),
            nonre_forc.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            nonre_forc.TimeToRecovery.cast("Int").alias("TimeToRecovery"),
            nonre_forc.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            nonre_forc.PortfolioID.cast("Int"),
            nonre_forc.BusinessDate.cast("Date"),
            lit("Business_Plan_Forc_CBS_NonRE").cast("String").alias("tbl"),
        )

        # ================== re_3rdparty_forc table schema ==================

        target_re_3rdparty_forc = re_3rdparty_forc.select(
            re_3rdparty_forc.BusinessPlan_ID.cast("Int").alias("BusinessPlan_ID"),
            re_3rdparty_forc.DebtorCDI.cast("String"),
            re_3rdparty_forc.AgreementNumber.cast("String"),
            re_3rdparty_forc.BusinessPlan_Status.cast("String"),
            re_3rdparty_forc.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            re_3rdparty_forc.BusinessPlan_SolutionType.cast("String"),
            re_3rdparty_forc.BusinessPlan_Comments.cast("String"),
            re_3rdparty_forc.BusinessPlanStrategy_Status.cast("String"),
            re_3rdparty_forc.Confidence.cast("String"),
            re_3rdparty_forc.ProposedBy.cast("String"),
            re_3rdparty_forc.BusinessPlanStrategy_StrategyType.cast("String"),
            re_3rdparty_forc.BusinessPlanStrategy_Comments.cast("String"),
            re_3rdparty_forc.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            re_3rdparty_forc.TimeToAuction.cast("Int").alias("TimeToAuction"),
            re_3rdparty_forc.AuctionPrice.cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            re_3rdparty_forc.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            re_3rdparty_forc.PortfolioID.cast("Int"),
            re_3rdparty_forc.BusinessDate.cast("Date"),
            lit("Business_Plan_Forc_CBS_RE_3rdParty").cast("String").alias("tbl"),
            )

        # ================== reo_hold_forc table schema ==================

        target_reo_hold_forc = reo_hold_forc.select(
            reo_hold_forc.BusinessPlan_ID.cast("Int").alias("BusinessPlan_ID"),
            reo_hold_forc.DebtorCDI.cast("String"),
            reo_hold_forc.AgreementNumber.cast("String"),
            reo_hold_forc.BusinessPlan_Status.cast("String"),
            reo_hold_forc.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            reo_hold_forc.BusinessPlan_SolutionType.cast("String"),
            reo_hold_forc.BusinessPlan_Comments.cast("String"),
            reo_hold_forc.BusinessPlanStrategy_Status.cast("String"),
            reo_hold_forc.Confidence.cast("String"),
            reo_hold_forc.ProposedBy.cast("String"),
            reo_hold_forc.BusinessPlanStrategy_StrategyType.cast("String"),
            reo_hold_forc.BusinessPlanStrategy_Comments.cast("String"),
            reo_hold_forc.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            reo_hold_forc.TimeToAuction.cast("Int").alias("TimeToAuction"),
            reo_hold_forc.AuctionPrice.cast("Decimal(19,2)").alias("AuctionPrice"),
            reo_hold_forc.TimeToSell.cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            reo_hold_forc.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            reo_hold_forc.PortfolioID.cast("Int"),
            reo_hold_forc.BusinessDate.cast("Date"),
            lit("Business_Plan_Forc_CBS_REO_Hold").cast("String").alias("tbl"),
        )

        # ================== reo_sale_forc table schema ==================

        target_reo_sale_forc = reo_sale_forc.select(
            reo_sale_forc.BusinessPlan_ID.cast("Int").alias("BusinessPlan_ID"),
            reo_sale_forc.DebtorCDI.cast("String"),
            reo_sale_forc.AgreementNumber.cast("String"),
            reo_sale_forc.BusinessPlan_Status.cast("String"),
            reo_sale_forc.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            reo_sale_forc.BusinessPlan_SolutionType.cast("String"),
            reo_sale_forc.BusinessPlan_Comments.cast("String"),
            reo_sale_forc.BusinessPlanStrategy_Status.cast("String"),
            reo_sale_forc.Confidence.cast("String"),
            reo_sale_forc.ProposedBy.cast("String"),
            reo_sale_forc.BusinessPlanStrategy_StrategyType.cast("String"),
            reo_sale_forc.BusinessPlanStrategy_Comments.cast("String"),
            reo_sale_forc.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            reo_sale_forc.TimeToAuction.cast("Int").alias("TimeToAuction"),
            reo_sale_forc.AuctionPrice.cast("Decimal(19,2)").alias("AuctionPrice"),
            reo_sale_forc.TimeToSell.cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            reo_sale_forc.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            reo_sale_forc.PortfolioID.cast("Int"),
            reo_sale_forc.BusinessDate.cast("Date"),
            lit("Business_Plan_Forc_CBS_REO_Sale").cast("String").alias("tbl"),
        )

        tool_forc_cbs = (
            target_nonre_forc
            .union(target_re_3rdparty_forc)
            .union(target_reo_hold_forc)
            .union(target_reo_sale_forc)
            )
        return tool_forc_cbs
    
    def businessPlanToolVol(self) -> SparkDataFrame:

        # ============================== cepalplatform_smevbusinessplandatavolcbsnonre ====================================

        nonre_vol_extra_filters = None
        if nonre_vol_extra_filters is not None:
            nonre_vol_extra_filters.append(self.date_filter)
            nonre_vol_final_filters = " AND ".join(
                nonre_vol_extra_filters
            )
        else:
            nonre_vol_final_filters = self.date_filter

        nonre_vol_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "TimeToRecovery",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        nonre_vol = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandatavolcbsnonre",
            nonre_vol_final_filters,
            nonre_vol_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandatavolcbsre3rdparty =======================================

        re_3rdparty_vol_extra_filters = None
        if re_3rdparty_vol_extra_filters is not None:
            re_3rdparty_vol_extra_filters.append(self.date_filter)
            re_3rdparty_vol_final_filters = " AND ".join(
                re_3rdparty_vol_extra_filters
            )
        else:
            re_3rdparty_vol_final_filters = self.date_filter

        re_3rdparty_vol_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "TimeToSell",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        re_3rdparty_vol = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandatavolcbsre3rdparty",
            re_3rdparty_vol_final_filters,
            re_3rdparty_vol_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandatavolcbsreohold =======================================

        reo_hold_vol_extra_filters = None
        if reo_hold_vol_extra_filters is not None:
            reo_hold_vol_extra_filters.append(self.date_filter)
            reo_hold_vol_final_filters = " AND ".join(
                reo_hold_vol_extra_filters
            )
        else:
            reo_hold_vol_final_filters = self.date_filter

        reo_hold_vol_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "TimeToSell",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        reo_hold_vol = load_landed_delta_table(
            self.spark,
            self.catalog_landed, 
            "cepalplatform_smevbusinessplandatavolcbsreohold",
            reo_hold_vol_final_filters,
            reo_hold_vol_selected_filters,
            )

        # ==================================== cepalplatform_smevbusinessplandatavolcbsreosale =======================================

        reo_sale_vol_extra_filters = None
        if reo_sale_vol_extra_filters is not None:
            reo_sale_vol_extra_filters.append(self.date_filter)
            reo_sale_vol_final_filters = " AND ".join(
                reo_sale_vol_extra_filters
            )
        else:
            reo_sale_vol_final_filters = self.date_filter

        reo_sale_vol_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "TimeToSell",
            "NetRecoveryAmount",
            "BusinessDate"
        ]

        reo_sale_vol = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandatavolcbsreosale",
            reo_sale_vol_final_filters,
            reo_sale_vol_selected_filters,
            )

        # ================== nonre_vol table schema ==================

        target_nonre_vol = nonre_vol.select(
            nonre_vol.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            nonre_vol.DebtorCDI.cast("String"),
            nonre_vol.AgreementNumber.cast("String"),
            nonre_vol.BusinessPlan_Status.cast("String"),
            nonre_vol.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            nonre_vol.BusinessPlan_SolutionType.cast("String"),
            nonre_vol.BusinessPlan_Comments.cast("String"),
            nonre_vol.BusinessPlanStrategy_Status.cast("String"),
            nonre_vol.Confidence.cast("String"),
            nonre_vol.ProposedBy.cast("String"),
            nonre_vol.BusinessPlanStrategy_StrategyType.cast("String"),
            nonre_vol.BusinessPlanStrategy_Comments.cast("String"),
            nonre_vol.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            nonre_vol.TimeToRecovery.cast("Int").alias("TimeToRecovery"),
            nonre_vol.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            nonre_vol.PortfolioID.cast("Int"),
            nonre_vol.BusinessDate.cast("Date"),
            lit("Business_Plan_Vol_CBS_NonRE").cast("String").alias("tbl"),
        )

        # ================== re_3rdparty_vol table schema ==================

        target_re_3rdparty_vol = re_3rdparty_vol.select(
            re_3rdparty_vol.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            re_3rdparty_vol.DebtorCDI.cast("String"),
            re_3rdparty_vol.AgreementNumber.cast("String"),
            re_3rdparty_vol.BusinessPlan_Status.cast("String"),
            re_3rdparty_vol.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            re_3rdparty_vol.BusinessPlan_SolutionType.cast("String"),
            re_3rdparty_vol.BusinessPlan_Comments.cast("String"),
            re_3rdparty_vol.BusinessPlanStrategy_Status.cast("String"),
            re_3rdparty_vol.Confidence.cast("String"),
            re_3rdparty_vol.ProposedBy.cast("String"),
            re_3rdparty_vol.BusinessPlanStrategy_StrategyType.cast("String"),
            re_3rdparty_vol.BusinessPlanStrategy_Comments.cast("String"),
            re_3rdparty_vol.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            re_3rdparty_vol.TimeToSell.cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            re_3rdparty_vol.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            re_3rdparty_vol.PortfolioID.cast("Int"),
            re_3rdparty_vol.BusinessDate.cast("Date"),
            lit("Business_Plan_Vol_CBS_RE_3rdParty").cast("String").alias("tbl"),
        )

        # ================== reo_hold_vol table schema ==================

        target_reo_hold_vol = reo_hold_vol.select(
            reo_hold_vol.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            reo_hold_vol.DebtorCDI.cast("String"),
            reo_hold_vol.AgreementNumber.cast("String"),
            reo_hold_vol.BusinessPlan_Status.cast("String"),
            reo_hold_vol.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            reo_hold_vol.BusinessPlan_SolutionType.cast("String"),
            reo_hold_vol.BusinessPlan_Comments.cast("String"),
            reo_hold_vol.BusinessPlanStrategy_Status.cast("String"),
            reo_hold_vol.Confidence.cast("String"),
            reo_hold_vol.ProposedBy.cast("String"),
            reo_hold_vol.BusinessPlanStrategy_StrategyType.cast("String"),
            reo_hold_vol.BusinessPlanStrategy_Comments.cast("String"),
            reo_hold_vol.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            reo_hold_vol.TimeToSell.cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            reo_hold_vol.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            reo_hold_vol.PortfolioID.cast("Int"),
            reo_hold_vol.BusinessDate.cast("Date"),
            lit("Business_Plan_Vol_CBS_REO_Hold").cast("String").alias("tbl"),
        )

        # ================== reo_sale_vol table schema ===================================

        target_reo_sale_vol = reo_sale_vol.select(
            reo_sale_vol.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            reo_sale_vol.DebtorCDI.cast("String"),
            reo_sale_vol.AgreementNumber.cast("String"),
            reo_sale_vol.BusinessPlan_Status.cast("String"),
            reo_sale_vol.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            reo_sale_vol.BusinessPlan_SolutionType.cast("String"),
            reo_sale_vol.BusinessPlan_Comments.cast("String"),
            reo_sale_vol.BusinessPlanStrategy_Status.cast("String"),
            reo_sale_vol.Confidence.cast("String"),
            reo_sale_vol.ProposedBy.cast("String"),
            reo_sale_vol.BusinessPlanStrategy_StrategyType.cast("String"),
            reo_sale_vol.BusinessPlanStrategy_Comments.cast("String"),
            reo_sale_vol.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            reo_sale_vol.TimeToSell.cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            reo_sale_vol.NetRecoveryAmount.cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            reo_sale_vol.PortfolioID.cast("Int"),
            reo_sale_vol.BusinessDate.cast("Date"),
            lit("Business_Plan_Vol_CBS_REO_Sale").cast("String").alias("tbl"),
        )
        tool_vol_cbs = (
            target_nonre_vol
            .union(target_re_3rdparty_vol)
            .union(target_reo_hold_vol)
            .union(target_reo_sale_vol)
        )

        return tool_vol_cbs
    
    def businessPlanIndivBankruptcyStandStill(self) -> SparkDataFrame:

        # ============================== cepalplatform_smevbusinessplandataindivbankruptcy ====================================

        indiv_bankruptcy_extra_filters = None
        if indiv_bankruptcy_extra_filters is not None:
            indiv_bankruptcy_extra_filters.append(self.date_filter)
            indiv_bankruptcy_final_filters = " AND ".join(
                indiv_bankruptcy_extra_filters
            )
        else:
            indiv_bankruptcy_final_filters = self.date_filter

        indiv_bankruptcy_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_CreationDate",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "NumberOfInstallments",
            "Installment",
            "PrepaymentMonth",
            "PrepaymentAmount",
            "PeriodicityID",
            "PortfolioID",
            "BusinessDate",
            ]

        indiv_bankruptcy = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandataindivbankruptcy",
            indiv_bankruptcy_final_filters,
            indiv_bankruptcy_selected_filters,
            )

        indiv_bankruptcy = indiv_bankruptcy.withColumn(
            "PeriodicityName",
            when(col("PeriodicityID") == 1, "Μηνιαία")
            .when(col("PeriodicityID") == 2, "Διμηνιαία")
            .when(col("PeriodicityID") == 3, "Τρίμηνη")
            .when(col("PeriodicityID") == 4, "Εξάμηνη")
            .when(col("PeriodicityID") == 5, "Ετήσια")
            .otherwise(None)
        )

        # ============================== cepalplatform_smevbusinessplandatastandstill ====================================

        stand_still_extra_filters = None
        if stand_still_extra_filters is not None:
            stand_still_extra_filters.append(self.date_filter)
            stand_still_final_filters = " AND ".join(
                stand_still_extra_filters
            )
        else:
            stand_still_final_filters = self.date_filter

        stand_still_selected_filters = [
            "BusinessPlan_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_CreationDate",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "PortfolioID",
            "BusinessDate",
            ]

        stand_still = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smevbusinessplandatastandstill",
            stand_still_final_filters,
            stand_still_selected_filters,
            )

        # ================== indiv_bankruptcy table schema ===================================

        target_indiv_bankruptcy = indiv_bankruptcy.select(
            indiv_bankruptcy.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            indiv_bankruptcy.DebtorCDI.cast("String"),
            indiv_bankruptcy.AgreementNumber.cast("String"),
            indiv_bankruptcy.BusinessPlan_Status.cast("String"),
            indiv_bankruptcy.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            indiv_bankruptcy.BusinessPlan_SolutionType.cast("String"),
            indiv_bankruptcy.BusinessPlan_Comments.cast("String"),
            indiv_bankruptcy.BusinessPlanStrategy_Status.cast("String"),
            indiv_bankruptcy.Confidence.cast("String"),
            indiv_bankruptcy.ProposedBy.cast("String"),
            indiv_bankruptcy.BusinessPlanStrategy_StrategyType.cast("String"),
            indiv_bankruptcy.BusinessPlanStrategy_Comments.cast("String"),
            indiv_bankruptcy.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            indiv_bankruptcy.NumberOfInstallments.cast("Int").alias("NumberOfInstallments"),
            indiv_bankruptcy.PrepaymentMonth.cast("Date").alias("PrepaymentMonth"),
            indiv_bankruptcy.PrepaymentAmount.cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            indiv_bankruptcy.PeriodicityName.cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            lit(None).cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            indiv_bankruptcy.PortfolioID.cast("Int"),
            indiv_bankruptcy.BusinessDate.cast("Date"),
            lit("Business_Plan_Indv_Bankcruptcy").cast("String").alias("tbl"),
        )

        # ================== stand_still table schema ===================================

        target_stand_still = stand_still.select(
            stand_still.BusinessPlan_ID.cast("Int").alias("BusinessPlanID"),
            stand_still.DebtorCDI.cast("String"),
            stand_still.AgreementNumber.cast("String"),
            stand_still.BusinessPlan_Status.cast("String"),
            stand_still.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            stand_still.BusinessPlan_SolutionType.cast("String"),
            stand_still.BusinessPlan_Comments.cast("String"),
            stand_still.BusinessPlanStrategy_Status.cast("String"),
            stand_still.Confidence.cast("String"),
            stand_still.ProposedBy.cast("String"),
            stand_still.BusinessPlanStrategy_StrategyType.cast("String"),
            stand_still.BusinessPlanStrategy_Comments.cast("String"),
            stand_still.LastModifiedDate.cast("Date"),
            lit(None).cast("Date").alias("CollectionPlan_Month"),
            lit(None).cast("Decimal(19, 2)").alias("CollectionPlan_Amount"),
            lit(None).cast("String").alias("BusinessPlanStrategy_SolutionType"),
            lit(None).cast("Int").alias("NumberOfInstallments"),
            lit(None).cast("Date").alias("PrepaymentMonth"),
            lit(None).cast("Decimal(19, 2)").alias("PrepaymentAmount"),
            lit(None).cast("Decimal(10, 4)").alias("Interest"),
            lit(None).cast("String").alias("PeriodicityID"),
            lit(None).cast("String").alias("InterestPaymentPeriodicityID"),
            lit(None).cast("Decimal(19, 2)").alias("BalloonAmount"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            lit(None).cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            lit(None).cast("Date").alias("FirstInstallmentDate"),
            lit(None).cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            stand_still.PortfolioID.cast("Int"),
            stand_still.BusinessDate.cast("Date"),
            lit("Business_Plan_Stand_Still").cast("String").alias("tbl"),
        )

        indiv_bankruptcy_stand_still = target_indiv_bankruptcy.union(target_stand_still)
        return indiv_bankruptcy_stand_still
    
    def lbsPaymentsSchedule(self) -> SparkDataFrame:

        # ============================== cepalplatform_smebpsmlbspaymentschedule ====================================

        lbs_payments_schedule_extra_filters = None
        if lbs_payments_schedule_extra_filters is not None:
            lbs_payments_schedule_extra_filters.append(self.date_filter)
            lbs_payments_schedule_final_filters = " AND ".join(
                lbs_payments_schedule_extra_filters
            )
        else:
            lbs_payments_schedule_final_filters = self.date_filter

        lbs_payments_schedule_selected_filters = [
            "LBSParentID",
            "InstallmentSN",
            "InstallmentDate",
            "InstallmentTotalAmount",
            "InstallmentCapitalAmount",
            "InstallmentChargeAmount",
            "InstallmentN128Amount",
            "InstallmentTotalChargeAmount",
            "InstallmentRemainingCapitalAmount",
            "InstallmentNPVAmount",
            "InstallmentNPVCalculationDate",
            "CreationDate",
            "CreationUser",
            "IsActive",
            "BusinessDate",
            ]

        lbs_payments_schedule = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebpsmlbspaymentschedule",
            lbs_payments_schedule_final_filters,
            lbs_payments_schedule_selected_filters,
            )

        # Keys of the table: **LBSParentID, InstallmentSN, InstallmentDate (or CreationDate)**
        lbs_payments_schedule = lbs_payments_schedule.filter(col("IsActive") == True)

        # =============================== Target table =================================

        target_lbs_payments_schedule = lbs_payments_schedule.select(
            lbs_payments_schedule.LBSParentID.cast("Int"),
            lbs_payments_schedule.InstallmentSN.cast("Int"),
            lbs_payments_schedule.InstallmentDate.cast("Timestamp"),
            lbs_payments_schedule.InstallmentTotalAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentCapitalAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentChargeAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentN128Amount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentTotalChargeAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentRemainingCapitalAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentNPVAmount.cast("Decimal(18, 4)"),
            lbs_payments_schedule.InstallmentNPVCalculationDate.cast("Timestamp"),
            lbs_payments_schedule.CreationDate.cast("Timestamp"),
            lbs_payments_schedule.CreationUser.cast("String"),
            lbs_payments_schedule.IsActive.cast("Boolean"),
            lbs_payments_schedule.BusinessDate.cast("Date"),
        )
        return target_lbs_payments_schedule
    
    def businessPlanContract(self) -> SparkDataFrame:

        # ============================== cepalplatform_smebusinessplancontract ====================================

        business_plan_contract_extra_filters = None
        if business_plan_contract_extra_filters is not None:
            business_plan_contract_extra_filters.append(self.date_filter)
            business_plan_contract_final_filters = " AND ".join(
                business_plan_contract_extra_filters
            )
        else:
            business_plan_contract_final_filters = self.date_filter

        business_plan_contract_selected_filters = [
            "ID",
            "AgreementNumber",
            "BusinessPlanStrategyManagementID",
            "creationdate",
            "CurrencyCode",
            "BusinessDate",
            ]

        business_plan_contract = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebusinessplancontract",
            business_plan_contract_final_filters,
            business_plan_contract_selected_filters,
            )

        # ============================== cepalplatform_smebusinessplancontractloan ====================================

        business_plan_contract_loan_extra_filters = None
        if business_plan_contract_loan_extra_filters is not None:
            business_plan_contract_loan_extra_filters.append(self.date_filter)
            business_plan_contract_loan_final_filters = " AND ".join(
                business_plan_contract_loan_extra_filters
            )
        else:
            business_plan_contract_loan_final_filters = self.date_filter

        business_plan_contract_loan_selected_filters = [
            "AccountNumber",
            "LoanStatus",
            "BusinessPlanContractID"
            ]

        business_plan_contract_loan = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebusinessplancontractloan",
            business_plan_contract_loan_final_filters,
            business_plan_contract_loan_selected_filters,
            )

        # ============================== cepalplatform_smebusinessplancontractcollateral ====================================

        business_plan_contract_collateral_extra_filters = None
        if business_plan_contract_collateral_extra_filters is not None:
            business_plan_contract_collateral_extra_filters.append(self.date_filter)
            business_plan_contract_collateral_final_filters = " AND ".join(
                business_plan_contract_collateral_extra_filters
            )
        else:
            business_plan_contract_collateral_final_filters = self.date_filter

        business_plan_contract_collateral_selected_filters = [
            "ID",
            "BusinessPlanContractID",
            "ABCollateralID",
            "ThesisCollateralID",
            "OwnershipID",
            "ABPropertyKey",
            "ThesisPropertyKey",
            ]

        business_plan_contract_collateral = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebusinessplancontractcollateral",
            business_plan_contract_collateral_final_filters,
            business_plan_contract_collateral_selected_filters,
            )

        # ============================== joins ====================================

        business_plan_contract_loan_collateral = (
            business_plan_contract
            .join(
                business_plan_contract_loan,
                business_plan_contract.ID == business_plan_contract_loan.BusinessPlanContractID,
                "left"
            )
            .join(
                business_plan_contract_collateral,
                business_plan_contract.ID == business_plan_contract_collateral.BusinessPlanContractID,
                "left"
            )                       
        )

        # ============================== business_contract schema ====================================

        target_business_plan_contract_loan_collateral = business_plan_contract_loan_collateral.select(
            business_plan_contract.ID.cast("Int"),
            business_plan_contract.BusinessPlanStrategyManagementID.cast("Int").alias("BusinessPlanStrategy_ID"),
            business_plan_contract.AgreementNumber.cast("String").alias("Agreement_Number"),
            business_plan_contract.creationdate.cast("Date").alias("CreationDate"),
            business_plan_contract_loan.AccountNumber.alias("Account_Number"),
            business_plan_contract_collateral.ABCollateralID.cast("String"),
            business_plan_contract_collateral.ThesisCollateralID.cast("String"),
            business_plan_contract_collateral.OwnershipID.cast("String"),
            business_plan_contract_collateral.ABPropertyKey.cast("String"),
            business_plan_contract_collateral.ThesisPropertyKey.cast("String"),
            business_plan_contract.CurrencyCode.cast("String"),
            business_plan_contract_loan.LoanStatus.cast("String"),
            business_plan_contract.BusinessDate.cast("Date"),
        )

        return target_business_plan_contract_loan_collateral
    
    def businessPlan(self) -> SparkDataFrame:

        # ============================== cepalplatform_smebusinessplan ====================================

        business_plan_extra_filters = None
        if business_plan_extra_filters is not None:
            business_plan_extra_filters.append(self.date_filter)
            business_plan_final_filters = " AND ".join(
                business_plan_extra_filters
            )
        else:
            business_plan_final_filters = self.date_filter

        business_plan_selected_filters = [
            "ID",
            "StatusID",
            "SolutionTypeID",
            "DebtorCDI",
            "DebtorAFM",
            "DebtorBankGroupID",
            "IntentionDate",
            "CompletionTargetDate",
            "ApplicationDate",
            "Comments",
            "CreationDate",
            "CreationUser",
            "LastModifiedDate",
            "LastModifiedUser",
            "PortfolioID",
            "BusinessDate",
            ]

        business_plan = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebusinessplan",
            business_plan_final_filters,
            business_plan_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpstatus ====================================

        business_plan_status_extra_filters = None
        if business_plan_status_extra_filters is not None:
            business_plan_status_extra_filters.append(self.date_filter)
            business_plan_status_final_filters = " AND ".join(
                business_plan_status_extra_filters
            )
        else:
            business_plan_status_final_filters = self.date_filter

        business_plan_status_selected_filters = ["ID", "Label"]
        business_plan_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpstatus",
            business_plan_status_final_filters,
            business_plan_status_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpsolutiontype ====================================

        business_plan_solution_type_extra_filters = None
        if business_plan_solution_type_extra_filters is not None:
            business_plan_solution_type_extra_filters.append(self.date_filter)
            business_plan_solution_type_final_filters = " AND ".join(
                business_plan_solution_type_extra_filters
            )
        else:
            business_plan_solution_type_final_filters = self.date_filter

        business_plan_solution_type_selected_filters = ["ID", "Label"]
        business_plan_solution_type = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpsolutiontype",
            business_plan_solution_type_final_filters,
            business_plan_solution_type_selected_filters,
            )

        # ============================== joins =======================================

        business_plan_status_solution_type = (
            business_plan
            .join(
                business_plan_status,
                business_plan.StatusID == business_plan_status.ID,
                "left"
            )
            .join(
                business_plan_solution_type,
                business_plan.SolutionTypeID == business_plan_solution_type.ID,
                "left"
            )
        )

        # ============================== target table =======================================

        target_business_plan_status_solution_type = business_plan_status_solution_type.select(
            business_plan.ID.cast("Int"),
            business_plan.DebtorCDI.cast("String"),
            business_plan.DebtorAFM.cast("String"),
            business_plan.DebtorBankGroupID.cast("String").alias("GroupCode"),
            business_plan_status.Label.cast("String").alias("Business_plan_status"),
            business_plan_solution_type.Label.cast("String").alias("SolutionType"), 
            business_plan.IntentionDate.cast("Date"), 
            business_plan.CompletionTargetDate.cast("Date"),
            business_plan.ApplicationDate.cast("Date"),
            business_plan.Comments.cast("String"),
            business_plan.CreationDate.cast("Timestamp"),
            business_plan.CreationUser.cast("String"),
            business_plan.LastModifiedDate.cast("Timestamp"),
            business_plan.LastModifiedUser.cast("String"),
            business_plan.PortfolioID.cast("Int"),
            business_plan.BusinessDate.cast("Date"),
        )

        return target_business_plan_status_solution_type
    
    def businessPlanStrategy(self) -> SparkDataFrame:

        # ============================== cepalplatform_smebusinessplanstrategymanagement ====================================

        business_plan_strategy_management_extra_filters = None
        if business_plan_strategy_management_extra_filters is not None:
            business_plan_strategy_management_extra_filters.append(self.date_filter)
            business_plan_strategy_management_final_filters = " AND ".join(
                business_plan_strategy_management_extra_filters
            )
        else:
            business_plan_strategy_management_final_filters = self.date_filter

        business_plan_strategy_management_selected_filters = [
            "ID",
            "StrategyID",
            "StatusID",
            "BusinessPlanID",
            "DraftedByID",
            "ProposedByID",
            "ConfidenceID",
            "BeginMonth",
            "Comments",
            "CreationDate",
            "CreationUser",
            "LastModifiedDate",
            "LastModifiedUser",
            "BusinessDate",
            ]

        business_plan_strategy_management = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smebusinessplanstrategymanagement",
            business_plan_strategy_management_final_filters,
            business_plan_strategy_management_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpstrategymanagementstatus ====================================

        business_plan_strategy_management_status_extra_filters = None
        if business_plan_strategy_management_status_extra_filters is not None:
            business_plan_strategy_management_status_extra_filters.append(self.date_filter)
            business_plan_strategy_management_status_final_filters = " AND ".join(
                business_plan_strategy_management_status_extra_filters
            )
        else:
            business_plan_strategy_management_status_final_filters = self.date_filter

        business_plan_strategy_management_status_selected_filters = [
            "ID",
            "Label",
            "Priority",
            "BusinessDate",
            ]

        business_plan_strategy_management_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpstrategymanagementstatus",
            business_plan_strategy_management_status_final_filters,
            business_plan_strategy_management_status_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpdraftedby ====================================

        business_plan_drafted_by_extra_filters = None
        if business_plan_drafted_by_extra_filters is not None:
            business_plan_drafted_by_extra_filters.append(self.date_filter)
            business_plan_drafted_by_final_filters = " AND ".join(
                business_plan_drafted_by_extra_filters
            )
        else:
            business_plan_drafted_by_final_filters = self.date_filter

        business_plan_drafted_by_selected_filters = ["ID", "Label", "BusinessDate"]
        business_plan_drafted_by = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpdraftedby",
            business_plan_drafted_by_final_filters,
            business_plan_drafted_by_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpproposedby ====================================

        business_plan_proposed_by_extra_filters = None
        if business_plan_proposed_by_extra_filters is not None:
            business_plan_proposed_by_extra_filters.append(self.date_filter)
            business_plan_proposed_by_final_filters = " AND ".join(
                business_plan_proposed_by_extra_filters
            )
        else:
            business_plan_proposed_by_final_filters = self.date_filter

        business_plan_proposed_by_selected_filters = ["ID", "Label", "BusinessDate"]
        business_plan_proposed_by = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpproposedby",
            business_plan_proposed_by_final_filters,
            business_plan_proposed_by_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpconfidence ====================================

        business_plan_confidence_extra_filters = None
        if business_plan_confidence_extra_filters is not None:
            business_plan_confidence_extra_filters.append(self.date_filter)
            business_plan_confidence_final_filters = " AND ".join(
                business_plan_confidence_extra_filters
            )
        else:
            business_plan_confidence_final_filters = self.date_filter

        business_plan_confidence_selected_filters = ["ID", "Label", "BusinessDate"]
        business_plan_confidence = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpconfidence",
            business_plan_confidence_final_filters,
            business_plan_confidence_selected_filters,
            )

        # ============================== cepalplatform_smerfdtbpstrategy ====================================

        business_plan_strategy_extra_filters = None
        if business_plan_strategy_extra_filters is not None:
            business_plan_strategy_extra_filters.append(self.date_filter)
            business_plan_strategy_final_filters = " AND ".join(
                business_plan_strategy_extra_filters
            )
        else:
            business_plan_strategy_final_filters = self.date_filter

        business_plan_strategy_selected_filters = ["ID", "Label", "BusinessDate"]
        business_plan_strategy = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtbpstrategy",
            business_plan_strategy_final_filters,
            business_plan_strategy_selected_filters,
            )

        # ============================== joins ====================================

        business_plan_strategy_management_joins = (
            business_plan_strategy_management
            .join(
                business_plan_strategy_management_status,
                business_plan_strategy_management_status.ID == business_plan_strategy_management.StatusID,
                "left"
                )
            .join(
                business_plan_drafted_by,
                business_plan_drafted_by.ID == business_plan_strategy_management.DraftedByID,
                "left"
                )
            .join(
                business_plan_strategy,
                business_plan_strategy.ID == business_plan_strategy_management.StrategyID,
                "left"
                )
            .join(
                business_plan_proposed_by,
                business_plan_proposed_by.ID == business_plan_strategy_management.ProposedByID,
                "left"
                )
            .join(
                business_plan_confidence,
                business_plan_confidence.ID == business_plan_strategy_management.ConfidenceID,
                "left"
            )

        )

        target_business_plan_strategy_management = business_plan_strategy_management_joins.select(
            business_plan_strategy_management.ID.cast("Int"),
            business_plan_strategy_management.BusinessPlanID.cast("Int"),
            business_plan_strategy_management_status.Label.cast("String").alias("BPSStatus"),
            business_plan_strategy_management_status.Priority.cast("Int").alias("BPSStatusPriority"),
            business_plan_drafted_by.Label.cast("String").alias("DraftBy"),
            business_plan_strategy.Label.cast("String").alias("Strategy"),
            business_plan_strategy_management.BeginMonth.cast("Date"),
            business_plan_proposed_by.Label.cast("String").alias("ProposedBy"),
            business_plan_confidence.Label.cast("String").alias("Confidence"),
            business_plan_strategy_management.Comments.cast("String"),
            business_plan_strategy_management.CreationDate.cast("Timestamp"),
            business_plan_strategy_management.CreationUser.cast("String"),
            business_plan_strategy_management.LastModifiedDate.cast("Timestamp"),
            business_plan_strategy_management.LastModifiedUser.cast("String"),
            business_plan_strategy_management.StrategyID.cast("Int"),
            business_plan_strategy_management.BusinessDate.cast("Date"), 
        )

        return target_business_plan_strategy_management
    
    def settlementPerimeter(self) -> SparkDataFrame:

        settlement_perimeter_extra_filters = None
        if settlement_perimeter_extra_filters is not None:
            settlement_perimeter_extra_filters.append(self.date_filter)
            settlement_perimeter_final_filters = " AND ".join(
                settlement_perimeter_extra_filters
                )
        else:
            settlement_perimeter_final_filters = self.date_filter

        settlement_perimeter_selected_fields = [
            "LBS_ID",
            "DebtorCDI",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_Code",
            "BusinessPlan_SolutionType",
            "BusinessPlan_Comments",
            "BusinessPlanStrategy_Status",
            "Confidence",
            "ProposedBy",
            "BusinessPlanStrategy_StrategyType",
            "BusinessPlanStrategy_Comments",
            "LastModifiedDate",
            "CollectionPlan_Month",
            "CollectionPlan_Amount",
            "BusinessPlanStrategy_SolutionType",
            "NumberOfInstallments",
            "PrepaymentMonth",
            "PrepaymentAmount",
            "Interest",
            "PeriodicityID",
            "InterestPaymentPeriodicityID",
            "BalloonAmount",
            "FirstInstallmentDate",
            "StartingCapitalAmn",
            "PortfolioID",
            "BusinessDate",
        ]
        
        catalog_landed  = "`p-cdh`.landed" 
        settlement_perimeter = load_landed_delta_table(
            self.spark,
            catalog_landed,  
            "sme_business_plan_perimeter_lbs_settlement_final",
            settlement_perimeter_final_filters,
            settlement_perimeter_selected_fields,
            )

        target_settlement_perimeter = settlement_perimeter.select(
            settlement_perimeter.LBS_ID.cast("Int").alias("BusinessPlanID"),
            settlement_perimeter.DebtorCDI.cast("String"),
            settlement_perimeter.AgreementNumber.cast("String"),
            settlement_perimeter.BusinessPlan_Status.cast("String"),
            settlement_perimeter.BusinessPlanStrategy_Code.cast("Int").alias("BP_Strategy_Code"),
            settlement_perimeter.BusinessPlan_SolutionType.cast("String"),
            settlement_perimeter.BusinessPlan_Comments.cast("String"),
            settlement_perimeter.BusinessPlanStrategy_Status.cast("String"),
            settlement_perimeter.Confidence.cast("String"),
            settlement_perimeter.ProposedBy.cast("String"),
            settlement_perimeter.BusinessPlanStrategy_StrategyType.cast("String"),
            settlement_perimeter.BusinessPlanStrategy_Comments.cast("String"),
            settlement_perimeter.LastModifiedDate.cast("Date"),
            settlement_perimeter.CollectionPlan_Month.cast("Date"),
            settlement_perimeter.CollectionPlan_Amount.cast("Decimal(19, 2)"),
            settlement_perimeter.BusinessPlanStrategy_SolutionType.cast("String"),
            settlement_perimeter.NumberOfInstallments.cast("Int"),
            settlement_perimeter.PrepaymentMonth.cast("Date"),
            settlement_perimeter.PrepaymentAmount.cast("Decimal(19, 2)"),
            settlement_perimeter.Interest.cast("Decimal(10, 4)"),
            settlement_perimeter.PeriodicityID.cast("String"),
            settlement_perimeter.InterestPaymentPeriodicityID.cast("String"),
            settlement_perimeter.BalloonAmount.cast("Decimal(19, 2)"),
            lit(None).cast("Int").alias("TimeToAuction"),
            lit(None).cast("Decimal(19,2)").alias("AuctionPrice"),
            lit(None).cast("Int").alias("TimeToSell"),
            lit(None).cast("Int").alias("TimeToRecovery"),
            lit(None).cast("Decimal(19, 2)").alias("NetRecoveryAmount"),
            lit(None).cast("String").alias("ImplementationStatus"),
            settlement_perimeter.FirstInstallmentDate.cast("Date"),
            settlement_perimeter.StartingCapitalAmn.cast("Decimal(19, 2)").alias("StartingCapitalAmount"),
            settlement_perimeter.PortfolioID.cast("Int"),
            settlement_perimeter.BusinessDate.cast("Date"),
            lit("Business_Plan_Perimeter_LBS_Settlement_final").cast("String").alias("tbl")
        )
        return target_settlement_perimeter
    
    def fetchMostRecentStrategy(self, source_table: SparkDataFrame) -> SparkDataFrame:

        """Documentation to be included"""

        window_spec = Window.partitionBy(
            "BusinessPlanID",
            "AgreementNumber",
            "BusinessPlan_Status",
            "BusinessPlanStrategy_StrategyType"
        ).orderBy(desc("BP_Strategy_Code"))

        source_table = source_table.withColumn(
            "rnk", row_number().over(window_spec)
        )

        target_table = source_table.filter(col("rnk") == 1).drop("rnk")
        return target_table
    
    def loadLandedSMETable(self, sme_table: str) -> SparkDataFrame:

        """For testing purposes"""
        
        sme_table = spark.read.table(f"`p-cdh`.landed.{sme_table}").filter(self.date_filter)
        for col_name, col_type in sme_table.dtypes:
            if col_type == "string":
                sme_table = sme_table.withColumn(col_name, trim(sme_table[col_name]))

        return sme_table
