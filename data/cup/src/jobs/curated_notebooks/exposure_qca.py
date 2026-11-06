# Databricks notebook source
from typing import Any

# COMMAND ----------

import logging

from py4j.java_gateway import JavaObject  # type: ignore
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    length,
    lit,
    max,
    sha2,
    substring,
    when,
)

# COMMAND ----------

dbutils.widgets.text('curated_zone', 'None', '01-Curated_zone')
dbutils.widgets.text('catalog_landed', 'None', '02-Catalog_landed')
dbutils.widgets.text('reference_zone', 'None', '03-Reference_zone')
dbutils.widgets.text('run_date', 'None', '04-Run Date')
dbutils.widgets.text('schema', 'None', '05-Schema')
dbutils.widgets.text('env_prefix', 'None', '06-Environment')
curated_zone = dbutils.widgets.get('curated_zone')
catalog_landed = dbutils.widgets.get('catalog_landed')
reference_zone = dbutils.widgets.get('reference_zone')
run_date = dbutils.widgets.get('run_date')
schema = dbutils.widgets.get('schema')
env_prefix = dbutils.widgets.get('env_prefix')

import sys, os
relative_path = '../../helpers/lng_lnd'
path = os.path.abspath(relative_path)
if path not in sys.path:
  sys.path.append(path)
from landing_helpers import *
from landed_helpers import *
relative_path = '../../helpers/curated_helpers'
path = os.path.abspath(relative_path)
if path not in sys.path:
  sys.path.append(path)
from delta_table_creator import *
from extract import *
from transform import *
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

def load_curated_exposure_qca(
    spark, curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix
) -> None:
    args = [curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix]
    if all(item is not None for item in args) :
        date_filter = f"businessdate = {run_date!r}"

        cluster_columns = ["BusinessDate"]

        curated_table_path = f"{curated_zone}.exposure"

        # ================== Create Delta Table ==================
        logging.info("Creating Delta Table...")
        if spark.catalog.tableExists(curated_table_path) is False:
            create_delta_table(
                spark,
                curated_table_path,
                cluster_columns,
                schema,  # type: ignore
            )
        else:
            logging.info("Table already exists.")

        # ================== Delete From Delta Table ==================
        if spark.catalog.tableExists(curated_table_path) is True:
            delete_from_curated_table(
                    spark,
                    curated_table_path,
                    run_date,  # type: ignore
                    "qca",
                )        
        else:
            logging.info("Tables does not exist, no need to delete entries for business date!")

        # ================== dim_cases_static ==================

        dim_cases_static_extra_filters = None

        if dim_cases_static_extra_filters is not None:
            dim_cases_static_extra_filters.append(date_filter)
            dim_cases_static_final_filters = " AND ".join(
                dim_cases_static_extra_filters
            )
        else:
            dim_cases_static_final_filters = date_filter

        dim_cases_static_selected_fields = [
            "CASE_ACCOUNT_FOR_SEARCH",
            "CASE_ACCOUNT_OPEN_DATE",
            "case_code",
            "case_code_primary",
            "CASE_CONTRACT_ACCOUNT_KEY",
            "CASE_CURRENTLY_OPEN",
            "CASE_EXP_DATE",
            "CAEX_CUSTOM_TEXT_22",
            "CASE_GENERIC_STATUS_03_DATE",
            "CASE_LEGAL_CONTRACT",
            "CASE_PERIODICITY",
            "CASE_S_ID",
            "CASE_UNP_BILL_NUM",
            "cust_code",
            "debt_code",
            "CASE_BU",
            "CASE_RELATION_TYPE_CODE",
            "CASE_APPLIC_NUM",
            "CASE_BRANCH_INTCODE",
            "CSTM_TEXT_AD_20", #new
            "CAEX_CUSTOM_TEXT_16",
            "CASE_SOLD_FLAG",
            "businessdate",
        ]

        dim_cases_static = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_cases_static",
            dim_cases_static_final_filters,
            dim_cases_static_selected_fields,
        )
        
        # ================== fsad_cases_generic_open ==================

        fsad_cases_generic_open_extra_filters = None

        if fsad_cases_generic_open_extra_filters is not None:
            fsad_cases_generic_open_extra_filters.append(date_filter)
            fsad_cases_generic_open_final_filters = " AND ".join(fsad_cases_generic_open_extra_filters)
        else:
            fsad_cases_generic_open_final_filters = date_filter

        fsad_cases_generic_open_extra_selected_fields = [
            "case_code", 
            "cust_code",
            "debt_code",
            "casg_generic_bit_01",
            "casg_generic_bit_02",
            "casg_generic_nvarchar_01",
        ]

        fsad_cases_generic_open = load_landed_delta_table(
            spark,
            catalog_landed, 
            "qca_fsad_cases_generic_open",
            fsad_cases_generic_open_final_filters,
            fsad_cases_generic_open_extra_selected_fields,
        )
        
        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "cust_code", "fsad_cases_generic_open_cust_code"
        )

        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "debt_code", "fsad_cases_generic_open_debt_code"
        )
        
        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "case_code", "fsad_cases_generic_open_case_code"
        )

        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "casg_generic_bit_01", "ForcedOnboarding"
        )
        
        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "casg_generic_bit_02", "HolisticFlag"
        )
        
        fsad_cases_generic_open = fsad_cases_generic_open.withColumnRenamed(
            "casg_generic_nvarchar_01", "ForcedOnboardReason"
        )
        
        fsad_cases_generic_open = fsad_cases_generic_open.dropDuplicates(
            subset = ["fsad_cases_generic_open_cust_code", "fsad_cases_generic_open_debt_code", "fsad_cases_generic_open_case_code"]
        )

        # ================== fs_cases_snap ==================

        fs_cases_snap_extra_filters = None

        if fs_cases_snap_extra_filters is not None:
            fs_cases_snap_extra_filters.append(date_filter)
            fs_cases_snap_final_filters = " AND ".join(fs_cases_snap_extra_filters)
        else:
            fs_cases_snap_final_filters = date_filter

        fs_cases_snap_selected_fields = [
            "case_code",
            "cust_code",
            "debt_code",
            "CAEX_CUSTOM_DECIMAL_07",
            "CAEX_CUSTOM_DECIMAL_09",
            "CAEX_CUSTOM_DECIMAL_12",
            "CAEX_CUSTOM_TEXT_30",
            "CASE_LEGAL_PROCESS_STATUS_ACTIVITY_DATE",
            "CASE_BILL_DATE",
            "CASE_CURRENCY",
            "CASE_DATE",
            "CASE_DELINQUENT_DAYS",
            "DEBT_LEGAL_PROCESS_STATUS_CODE",
            "CAEX_CUSTOM_DATETIME_11",
            "CAEX_CUSTOM_DATETIME_03",
            "CASE_INTERNAL_BU",
            "CASE_DENOUNCE_DATE",
            "CASE_DENOUNCE_STATUS",
            "CASE_DUE_DATE",
            "CASE_OBJECTIVE_STATUS",
            "CASE_WR_OF_DATE",
            "CSTM_DECIMAL_06",
            "CAEX_CUSTOM_DATETIME_12",
            "CSTM_DECIMAL_07",
            "SPRD_CODE",
            "CASE_LEGAL_PROCESS_STATUS",
            "CAEX_CUSTOM_TEXT_44",
            "CAEX_CUSTOM_INT_14",
            "CSTM_TEXT_10",
            "CASE_DEBT_AMOUNT",
            "CASE_OUTST_BALANCE",
            "CASE_CUSTOM_DECIMAL_01",
            "CASE_WR_OF_AMOUNT",
            "BRN_CODE_TREATMENT",
            "CAEX_CUSTOM_DATETIME_25",
            "CAEX_CUSTOM_DATETIME_26",
            "CAEX_CUSTOM_DATETIME_27",
            "CAEX_CUSTOM_DATETIME_30",
            "CASP_GENERIC_INT_05_CODE",
            "SPET_CODE",
            "CASE_SOURCE",
            "CASE_EXP_DATE", #new
            "CASE_RELATION_TYPE_CODE",#new
            "CAEX_CUSTOM_DATETIME_31",#new
            "CAEX_CUSTOM_INT_19",#new
            "CAEX_CUSTOM_BIT_07",#new
            "CAEX_CUSTOM_BIT_08",#new
            "CAEX_CUSTOM_BIT_09",#new
            "CAEX_CUSTOM_BIT_11",#new
            "CASP_GENERIC_INT_05_CODE_DATE",#new
            "DEBT_CODE",#new
            "CASE_LEGAL_PROCESS_STATUS_UPD",#new
            "CASE_BUCKET",#new
        ]

        fs_cases_snap = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fs_cases_snap",
            fs_cases_snap_final_filters,
            fs_cases_snap_selected_fields,
        )

        # ================== fs_cases_snap_extra ==================

        fs_cases_snap_extra_extra_filters = None

        if fs_cases_snap_extra_extra_filters is not None:
            fs_cases_snap_extra_extra_filters.append(date_filter)
            fs_cases_snap_extra_final_filters = " AND ".join(
                fs_cases_snap_extra_extra_filters
            )
        else:
            fs_cases_snap_extra_final_filters = date_filter

        fs_cases_snap_extra_selected_fields = [
            "case_code",
            "CEXT_GENERIC_DATETIME_04",
            "CEXT_GENERIC_BIT_02",
            "CEXT_GENERIC_BIT_03",
        ]

        fs_cases_snap_extra = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fs_cases_snap_extra",
            fs_cases_snap_extra_final_filters,
            fs_cases_snap_extra_selected_fields,
        )

        # ================== fsad_debts ==================

        fsad_debts_extra_filters = None

        if fsad_debts_extra_filters is not None:
            fsad_debts_extra_filters.append(date_filter)
            fsad_debts_final_filters = " AND ".join(fsad_debts_extra_filters)
        else:
            fsad_debts_final_filters = date_filter

        fsad_debts_selected_fields = [
            "DEBT_CODE",
            "DEBT_CAPITAL_AMT",
            "DEBT_CLAIM_CLAIMED_PI_AMT",
            "DEBT_LEGAL_PROCESS_STATUS_ACTIVITY_DATE",
            "DEBT_CLAIM_PAID_I_AMT",
            "DEBT_DELINQUENT_DAYS",
            "DEBT_GENERIC_STATUS_04_DATE",
            "DEBT_OUTSTANDING_AMT",
            "DEBT_BUCKET_CODE",
            "DEBT_LEGAL_PROCESS_STATUS_CODE",
            "DEBT_CURRENCY",#new
            "DEBT_ADMINISTRATION_STATUS_CODE", # new
            "SPRD_CODE", #new
        ]

        fsad_debts = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fsad_debts",
            fsad_debts_final_filters,
            fsad_debts_selected_fields,
        )

        # ================== dim_branch ==================

        dim_branch_extra_filters = None

        if dim_branch_extra_filters is not None:
            dim_branch_extra_filters.append(date_filter)
            dim_branch_final_filters = " AND ".join(dim_branch_extra_filters)
        else:
            dim_branch_final_filters = date_filter

        dim_branch_selected_fields = [
            "BRN_INTCODE",
            "BRN_ID",
            "BRN_DESC",
        ]

        dim_branch = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_branch",
            dim_branch_final_filters,
            dim_branch_selected_fields,
        )

        dim_branch_case = dim_branch.alias("dim_branch_case")
        dim_branch_case = (
            dim_branch_case.withColumnRenamed("BRN_INTCODE", "BRN_INTCODE_case")
            .withColumnRenamed("BRN_DESC", "BRN_DESC_case")
            .withColumnRenamed("BRN_ID", "BRN_ID_case")
        )

        dim_branch_treatment = dim_branch.alias("dim_branch_treatment")
        dim_branch_treatment = (
            dim_branch_treatment.withColumnRenamed(
                "BRN_INTCODE", "BRN_INTCODE_treatment"
            )
            .withColumnRenamed("BRN_DESC", "BRN_DESC_treatment")
            .withColumnRenamed("BRN_ID", "BRN_ID_treatment")
        )

        # ================== dim_bucket ==================

        dim_bucket_extra_filters = None

        if dim_bucket_extra_filters is not None:
            dim_bucket_extra_filters.append(date_filter)
            dim_bucket_final_filters = " AND ".join(dim_bucket_extra_filters)
        else:
            dim_bucket_final_filters = date_filter

        dim_bucket_selected_fields = [
            "BCK_ID",
            "BCK_S_ID",
            "BCK_DESC",
        ]

        dim_bucket = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_bucket",
            dim_bucket_final_filters,
            dim_bucket_selected_fields,
        )

        # ================== fsad_cases_extra_open ==================

        fsad_cases_extra_open_extra_filters = None

        if fsad_cases_extra_open_extra_filters is not None:
            fsad_cases_extra_open_extra_filters.append(date_filter)
            fsad_cases_extra_open_final_filters = " AND ".join(
                fsad_cases_extra_open_extra_filters
            )
        else:
            fsad_cases_extra_open_final_filters = date_filter

        fsad_cases_extra_open_selected_fields = [
            "CASE_CODE",
            "CAEX_CUSTOM_DECIMAL_28",
            "CAEX_CUSTOM_DECIMAL_29",
            "CAEX_CUSTOM_DECIMAL_66",
            "CAEX_CUSTOM_DECIMAL_34_LCY",
            "CAEX_CUSTOM_DECIMAL_30",
            "CAEX_CUSTOM_DECIMAL_66_LCY",
            "CAEX_CUSTOM_TEXT_08",
        ]

        fsad_cases_extra_open = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fsad_cases_extra_open",
            fsad_cases_extra_open_final_filters,
            fsad_cases_extra_open_selected_fields,
        )

        # ================== dim_customer_static ==================

        dim_customer_static_extra_filters = None

        if dim_customer_static_extra_filters is not None:
            dim_customer_static_extra_filters.append(date_filter)
            dim_customer_static_final_filters = " AND ".join(
                dim_customer_static_extra_filters
            )
        else:
            dim_customer_static_final_filters = date_filter

        dim_customer_static_selected_fields = [
            "CUST_ID",
            "cust_cif",
            "CUST_AFM",
            "cust_first_name",
            "cust_last_name",
        ]

        dim_customer_static = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_customer_static",
            dim_customer_static_final_filters,
            dim_customer_static_selected_fields,
        )

        # ================== dim_relationship_type ==================

        dim_relationship_type_extra_filters = None

        if dim_relationship_type_extra_filters is not None:
            dim_relationship_type_extra_filters.append(date_filter)
            dim_relationship_type_final_filters = " AND ".join(
                dim_relationship_type_extra_filters
            )
        else:
            dim_relationship_type_final_filters = date_filter

        dim_relationship_type_selected_fields = [
            "RT_DESC",
            "RT_ID",
        ]

        dim_relationship_type = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_relationship_type",
            dim_relationship_type_final_filters,
            dim_relationship_type_selected_fields,
        )

        # ================== fsad_case_billing ==================

        fsad_case_billing_extra_filters = None

        if fsad_case_billing_extra_filters is not None:
            fsad_case_billing_extra_filters.append(date_filter)
            fsad_case_billing_final_filters = " AND ".join(
                fsad_case_billing_extra_filters
            )
        else:
            fsad_case_billing_final_filters = date_filter

        fsad_case_billing_selected_fields = ["CASB_EOD_DATE", "CASE_CODE"]

        fsad_case_billing = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fsad_case_billing",
            fsad_case_billing_final_filters,
            fsad_case_billing_selected_fields,
        )

        fsad_case_billing = fsad_case_billing.distinct()

        fsad_case_billing = fsad_case_billing.groupBy("case_code").agg(
            max(col("CASB_EOD_DATE")).alias("CASB_EOD_DATE")
        )

        # ================== dim_subproduct ==================

        dim_subproduct_extra_filters = None

        if dim_subproduct_extra_filters is not None:
            dim_subproduct_extra_filters.append(date_filter)
            dim_subproduct_final_filters = " AND ".join(dim_subproduct_extra_filters)
        else:
            dim_subproduct_final_filters = date_filter

        dim_subproduct_selected_fields = [
            "SPRD_ID",
            "SPRDH1_ID",#new
        ]

        dim_subproduct = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_subproduct",
            dim_subproduct_final_filters,
            dim_subproduct_selected_fields,
        )

        # ================== dim_subproduct_hier1 ================== #new table
 
        dim_subproduct_hier1_extra_filters = None
 
        if dim_subproduct_hier1_extra_filters is not None:
            dim_subproduct_hier1_extra_filters.append(date_filter)
            dim_subproduct_hier1_final_filters = " AND ".join(dim_subproduct_hier1_extra_filters)
        else:
            dim_subproduct_hier1_final_filters = date_filter
 
        dim_subproduct_hier1_selected_fields = [
            "SPRDH1_ID",
        ]
 
        dim_subproduct_hier1 = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_dim_subproduct_hier1",
            dim_subproduct_hier1_final_filters,
            dim_subproduct_hier1_selected_fields,
        )
 


        # ===================fsad_list_of_values=========

        fsad_list_of_values_extra_filters = None

        if fsad_list_of_values_extra_filters is not None:
            fsad_list_of_values_extra_filters.append(date_filter)
            fsad_list_of_values_final_filters = " AND ".join(
                fsad_list_of_values_extra_filters #new - correction
            )
        else:
            fsad_list_of_values_final_filters = date_filter

        fsad_list_of_values_selected_fields = [
            "LOV_COMMENTS",
            "LOV_DESC",
            "LOVT_CODE",
            "LOV_CODE",
        ]

        fsad_list_of_values = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_fsad_list_of_values",
            fsad_list_of_values_final_filters,
            fsad_list_of_values_selected_fields,
        )

        fsad_list_of_values_2 = (
            fsad_list_of_values.withColumnRenamed("LOVT_CODE", "LOVT_CODE_2")
            .withColumnRenamed("LOV_DESC", "LOV_DESC_2")
            .withColumnRenamed("LOV_COMMENTS", "LOV_COMMENTS_2")
            .withColumnRenamed("LOV_CODE", "LOV_CODE_2")
        )

        # ================== portfolio ==================

        portfolio_extra_filters = None

        if portfolio_extra_filters is not None:
            portfolio_extra_filters.append(date_filter)
            portfolio_final_filters = " AND ".join(portfolio_extra_filters)
        else:
            portfolio_final_filters = date_filter  # possible need change date

        portfolio_selected_fields = [
            "PortfolioName",
            "portfolio_id_qca",
            "originBank",
        ]

        portfolio = load_landed_portfolio_table(  # noqa: F841
            spark,
            curated_zone,  # type: ignore
            "portfolio",
            portfolio_final_filters,
            portfolio_selected_fields,
        )

        # ================== at_cases_extension ==================

        at_cases_extension_extra_filters = None

        if at_cases_extension_extra_filters is not None:
            at_cases_extension_extra_filters.append(date_filter)
            at_cases_extension_final_filters = " AND ".join(
                at_cases_extension_extra_filters
            )
        else:
            at_cases_extension_final_filters = date_filter

        at_cases_extension_selected_fields = [
            "case_code",
            "cext_dedicated_property_08_nvarchar",
        ]

        at_cases_extension = load_landed_delta_table(
            spark,
            catalog_landed,  # type: ignore
            "qca_at_cases_extension",
            at_cases_extension_final_filters,
            at_cases_extension_selected_fields,
        )

        # ================== Join conditions ==================

        dim_cases_static_fs_cases_snap_join_condition = (
            dim_cases_static["CASE_CODE"] == fs_cases_snap["CASE_CODE"]
        )

        dim_cases_static_fs_cases_snap_extra_join_condition = (
            dim_cases_static["CASE_CODE"] == fs_cases_snap_extra["CASE_CODE"]
        )

        dim_cases_static_fsad_debts_join_condition = (
            dim_cases_static["DEBT_CODE"] == fsad_debts["DEBT_CODE"]
        )

        fs_cases_snap_fsad_list_of_values_join_condition_2 = (
            fs_cases_snap["CASP_GENERIC_INT_05_CODE"].cast("string")
            == fsad_list_of_values_2["LOV_CODE_2"]
        ) & (fsad_list_of_values_2["LOVT_CODE_2"] == 845)

        dim_cases_static_dim_relationship_type_join_condition = (
            dim_cases_static["CASE_RELATION_TYPE_CODE"]
            == dim_relationship_type["RT_ID"]
        )

        fsad_debts_dim_bucket_join_condition = (
            fsad_debts["DEBT_BUCKET_CODE"] == dim_bucket["BCK_ID"]
        )

        fs_cases_snap_dim_subproduct_status_join_condition = (
            fs_cases_snap["SPRD_CODE"] == dim_subproduct["SPRD_ID"]
        )

        dim_subproduct_dim_subproduct_hier1_status_join_condition = (
             dim_subproduct["SPRDH1_ID"] == dim_subproduct_hier1["SPRDH1_ID"]
         ) #new join

        dim_cases_static_dim_branch_case_join_condition = (
            dim_cases_static["CASE_BRANCH_INTCODE"]
            == dim_branch_case["BRN_INTCODE_case"]
        )

        fs_cases_snap_fsad_cases_extra_open_join_condition = (
            fs_cases_snap["case_code"] == fsad_cases_extra_open["CASE_CODE"]
        )

        dim_cases_static_dim_customer_static_join_condition = (
            dim_cases_static["cust_code"] == dim_customer_static["CUST_ID"]
        )

        fs_cases_snap_dim_branch_treatment_join_condition = fs_cases_snap[
            "BRN_CODE_TREATMENT"
        ].cast("string") == dim_branch_treatment["BRN_ID_treatment"].cast("string")

        fs_cases_snap_portfolio_join_condition = (
            fs_cases_snap["SPET_CODE"] == portfolio["portfolio_id_qca"]
        )

        fs_cases_snap_fsad_list_of_values_join_condition = (
            fs_cases_snap["CSTM_TEXT_10"] == fsad_list_of_values["LOV_DESC"]
        ) & (fsad_list_of_values["LOVT_CODE"] == 1221)

        dim_cases_static_fsad_case_billing_join_condition = (
            dim_cases_static["CASE_CODE"] == fsad_case_billing["CASE_CODE"]
        )

        dim_cases_static_at_cases_extension_join_condition = (
            dim_cases_static["CASE_CODE"] == at_cases_extension["CASE_CODE"]
        )
        
        fsad_cases_generic_open_join_condition = (
          fsad_cases_generic_open['fsad_cases_generic_open_case_code'] == fs_cases_snap["case_code"]
          ) & (fsad_cases_generic_open['fsad_cases_generic_open_cust_code'] == fs_cases_snap["cust_code"]
          ) & (fsad_cases_generic_open['fsad_cases_generic_open_debt_code'] == fs_cases_snap["debt_code"]) 

        # ================== Joins ==================
        target_table_qualco_ab = (
            dim_cases_static.join(
                fs_cases_snap,
                dim_cases_static_fs_cases_snap_join_condition,
                "inner",
            )
            .join(
                fsad_cases_generic_open,
                fsad_cases_generic_open_join_condition,
                "left"
            )
            .join(
                fs_cases_snap_extra,
                dim_cases_static_fs_cases_snap_extra_join_condition,
                "left",
            )
            .join(fsad_debts, dim_cases_static_fsad_debts_join_condition, "inner")
            .join(
                fsad_list_of_values_2,
                fs_cases_snap_fsad_list_of_values_join_condition_2,
                "left",
            )
            .join(
                dim_relationship_type,
                dim_cases_static_dim_relationship_type_join_condition,
                "left",
            )
            .join(dim_bucket, fsad_debts_dim_bucket_join_condition, "left")
            .join(
                dim_subproduct,
                fs_cases_snap_dim_subproduct_status_join_condition,
                "left",
            ).join (
                dim_subproduct_hier1,
                dim_subproduct_dim_subproduct_hier1_status_join_condition,
                "left",
            ) #new join
            .join(
                dim_branch_case, dim_cases_static_dim_branch_case_join_condition, "left"
            )
            .join(
                fsad_cases_extra_open,
                fs_cases_snap_fsad_cases_extra_open_join_condition,
                "left",
            )
            .join(
                dim_customer_static,
                dim_cases_static_dim_customer_static_join_condition,
                "left",
            )
            .join(
                dim_branch_treatment,
                fs_cases_snap_dim_branch_treatment_join_condition,
                "left",
            )
            .join(
                portfolio,
                fs_cases_snap_portfolio_join_condition,
                "left",
            )
            .join(
                fsad_list_of_values,
                fs_cases_snap_fsad_list_of_values_join_condition,
                "left",
            )
            .join(
                fsad_case_billing,
                dim_cases_static_fsad_case_billing_join_condition,
                "left",
            )
            .join(
                at_cases_extension,
                dim_cases_static_at_cases_extension_join_condition,
                "left",
            )
        )

        target_table_qualco_ab_ExposureId_primary_key_fields = [
            dim_cases_static.CASE_ACCOUNT_FOR_SEARCH,
            dim_cases_static.cust_code,
            dim_relationship_type.RT_DESC,
            lit("qca"),
        ]
        target_table_qualco_ab_ExposureDebtorKey_primary_key_fields = [
            dim_cases_static.CASE_ACCOUNT_FOR_SEARCH,
            dim_cases_static.cust_code,
            dim_relationship_type.RT_DESC,
        ]

        target_table_qualco_ab = (
            target_table_qualco_ab.withColumn(
                "ExposureId",
                sha2(
                    concat_ws(
                        ";", *target_table_qualco_ab_ExposureId_primary_key_fields
                    ),
                    256,
                ),
            )
            .withColumn(
                "ExposureDebtorKey",
                concat_ws(
                    ";", *target_table_qualco_ab_ExposureDebtorKey_primary_key_fields
                ),
            )
            .withColumn(                   #new
                "AgreementKey",
                when(
                    dim_cases_static.CSTM_TEXT_AD_20.isNull() | (trim(dim_cases_static.CSTM_TEXT_AD_20) == ""),
                    when(
                        (fs_cases_snap.CASE_SOURCE).like("C%"),
                        dim_cases_static.CASE_CONTRACT_ACCOUNT_KEY,
                    ).otherwise(dim_cases_static.CASE_LEGAL_CONTRACT)
                ).otherwise(dim_cases_static.CSTM_TEXT_AD_20)
            )
            .withColumn(
                "PrimaryPartyFlag",
                when(
                    dim_cases_static.case_code == dim_cases_static.case_code_primary, 1
                ).otherwise(0),
            )
            .withColumn(
                "ExposureType",
                when(
                    (fs_cases_snap.CAEX_CUSTOM_TEXT_30.isNull())
                    & (
                        ~fs_cases_snap.SPRD_CODE.isin(
                            "3112",
                            "0000003111",
                            "0000003110",
                            "0000003109",
                            "0000003114",
                            "000003120",
                            "000003115",
                            "000003119",
                            "000003117",
                            "000003121",
                            "000003122",
                            "000003123",
                            "000003124",
                            "000003127",
                            "000003149",
                            "000003148",
                            "000003147",
                            "000003126",
                            "000003152",
                            "000003110",
                            "000003111",
                        )
                    ),
                    "Account",
                ).otherwise("Slave Account"),
            )
            .withColumn(
                "ExposureStatus",
                when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-NON DELINQUENT",
                    lit("Normal"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-DELINQUENT",
                    lit("Overdue"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-DENOUNCED",
                    lit("Define Delay"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-WRITE OFF",
                    lit("Writen Off"),
                ),
            )
            .withColumn(
                "PartyName",
                when(
                    dim_customer_static.cust_first_name.isin(None),
                    dim_customer_static.cust_last_name,
                ).otherwise(
                    concat_ws(
                        " ",
                        dim_customer_static.cust_first_name,
                        dim_customer_static.cust_last_name,
                    )
                ),
            )
            .withColumn(
                "SlaveFlag",
                when(
                    fs_cases_snap.CAEX_CUSTOM_TEXT_30.isNull(),
                    False,
                ).otherwise(True),
            )
            .withColumn(
                "DenouncedStatus",
                when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-NON DELINQUENT",
                    lit("Normal"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-DELINQUENT",
                    lit("Overdue"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-DENOUNCED",
                    lit("Define Delay"),
                )
                .when(
                    fs_cases_snap.CASE_DENOUNCE_STATUS == "074-WRITE OFF",
                    lit("Writen Off"),
                ),
            )
            .withColumn(
                "LetterStatusCode",
                coalesce(
                    when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_27.cast("date")
                            != "1753-01-01"
                        ),
                        "Αποστολή 3ης επιστολής ΔΕΚ",
                    )
                    .when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_26.cast("date")
                            != "1753-01-01"
                        ),
                        "Αποστολή 2ης επιστολής ΔΕΚ",
                    )
                    .when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_25.cast("date")
                            != "1753-01-01"
                        ),
                        "Αποστολή 1ης επιστολής ΔΕΚ",
                    )
                    .otherwise(None)
                ),
            )
            .withColumn(
                "LetterStatusDate",
                coalesce(
                    when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_27.cast("date")
                            != "1753-01-01"
                        ),
                        fs_cases_snap.CAEX_CUSTOM_DATETIME_27,
                    )
                    .when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_26.cast("date")
                            != "1753-01-01"
                        ),
                        fs_cases_snap.CAEX_CUSTOM_DATETIME_26,
                    )
                    .when(
                        (
                            fs_cases_snap.CAEX_CUSTOM_DATETIME_25.cast("date")
                            != "1753-01-01"
                        ),
                        fs_cases_snap.CAEX_CUSTOM_DATETIME_25,
                    )
                    .otherwise(None)
                ),
            )
            .withColumn(
                "FirstLetterDate",
                when(
                    fs_cases_snap.CAEX_CUSTOM_DATETIME_25 == "1753-01-01 00:00:00.000",
                    lit(None),
                ).otherwise(fs_cases_snap.CAEX_CUSTOM_DATETIME_25),
            )
            .withColumn(
                "SecondLetterDate",
                when(
                    fs_cases_snap.CAEX_CUSTOM_DATETIME_26 == "1753-01-01 00:00:00.000",
                    lit(None),
                ).otherwise(fs_cases_snap.CAEX_CUSTOM_DATETIME_26),
            )
            .withColumn(
                "ThirdLetterDate",
                when(
                    fs_cases_snap.CAEX_CUSTOM_DATETIME_27 == "1753-01-01 00:00:00.000",
                    lit(None),
                ).otherwise(fs_cases_snap.CAEX_CUSTOM_DATETIME_27),
            )
            .withColumn(
                "FirstLetterReminderDate",
                when(
                    fs_cases_snap.CAEX_CUSTOM_DATETIME_30 == "1753-01-01 00:00:00.000",
                    lit(None),
                ).otherwise(fs_cases_snap.CAEX_CUSTOM_DATETIME_30),
            )
        )



        # ================== Selection of Final Columns ==================

        result_df_qualco_ab = target_table_qualco_ab.select(
            target_table_qualco_ab.ExposureId.alias("ExposureId"),
            lit("qca").alias("SourceSystem"),
            portfolio.portfolio_id_qca.cast("string").alias("PortfolioKey"),
            portfolio.PortfolioName.cast("string").alias("PortfolioName"),
            lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
                "IngestionTimestamp"
            ),
            lit(None).alias("BatchId"),
            target_table_qualco_ab.ExposureDebtorKey.alias("ExposureDebtorKey"),
            dim_cases_static.case_code.alias("CaseCode"),
            dim_cases_static.debt_code.cast("string").alias("ExposureSourceSystemKey"),
            dim_cases_static.CASE_ACCOUNT_FOR_SEARCH.cast("string").alias(
                "ExposureKey"
            ),
            target_table_qualco_ab.AgreementKey.cast("string").alias("AgreementKey"), #new
            dim_cases_static.cust_code.cast("string").alias("PartyKey"),
            dim_customer_static.cust_cif.alias("CDI"),
            target_table_qualco_ab.PartyName.cast("string").alias("PartyName"),
            dim_customer_static.CUST_AFM.alias("PartyTaxRegistrationNumber"),
            dim_relationship_type.RT_ID.cast("string").alias(
                "PartyRelationshipTypeCode"
            ),
            dim_relationship_type.RT_DESC.cast("string").alias("PartyRelationshipType"),
            target_table_qualco_ab.PrimaryPartyFlag.cast("boolean").alias(
                "PrimaryPartyFlag"
            ),
            dim_subproduct.SPRD_ID.cast("string").alias("ProductKey"), #new - SPRD_ID is SUBProduct, SPRDH1_ID is Product
            dim_subproduct.SPRD_ID.alias("ProductCode"), #new - SPRD_ID is SUBProduct, SPRDH1_ID is Product
            target_table_qualco_ab.ExposureType.alias("ExposureTypeCode"),
            target_table_qualco_ab.ExposureType.alias("ExposureType"),
            fs_cases_snap.CASE_DENOUNCE_STATUS.alias("ExposureStatusCode"),
            target_table_qualco_ab.ExposureStatus.cast("string").alias(
                "ExposureStatus"
            ),
            fs_cases_snap.CASE_DENOUNCE_DATE.cast("date").alias("ExposureStatusDate"),
            dim_cases_static.CASE_ACCOUNT_OPEN_DATE.cast("date").alias("OpenDate"),
            lit(None).alias("ActiveExpiryDate"),
            dim_cases_static.CASE_EXP_DATE.cast("date").alias("MaturityDate"),
            portfolio.originBank.cast("string").alias("OriginBank"),
            fsad_debts.DEBT_CLAIM_CLAIMED_PI_AMT.cast("decimal(19, 2)").alias(
                "OriginationAmount"
            ),
            fs_cases_snap.CASE_CURRENCY.cast("string").alias("Currency"),
            lit(None).cast("Int").alias("BankingUnitCode"),
            lit(None).cast("string").alias("BankingUnit"),
            lit(None).cast("string").alias("MechanismCode"),
            lit(None).cast("string").alias("Mechanism"),
            dim_branch_case.BRN_INTCODE_case.cast("string").alias("BranchCode"),
            dim_branch_case.BRN_DESC_case.cast("string").alias("Branch"),
            lit(None).cast("int").alias("PurposeReasonCode"),
            lit(None).cast("string").alias("PurposeReason"),
            lit(None).cast("int").alias("PurposeKTCode"),
            lit(None).cast("string").alias("PurposeKT"),
            lit(None).alias("MaterialityStartDate"),
            lit(None).alias("DaysPastDueWithMateriality"),
            fsad_debts.DEBT_DELINQUENT_DAYS.cast("int").alias("DaysPastDue"),
            fs_cases_snap.CASE_DELINQUENT_DAYS.cast("int").alias(
                "DaysPastDuePrincipal"
            ),
            lit(None).cast("string").alias("DaysPastDueInterest"),
            fsad_debts.DEBT_BUCKET_CODE.cast("string").alias("BucketCode"),
            dim_bucket.BCK_S_ID.cast("string").alias("Bucket"),
            lit(None).cast("date").alias("FirstArrearDate"),
            fs_cases_snap.CASE_DUE_DATE.cast("date").alias("ExpectedOverdueDate"),
            lit(None).cast("date").alias("OverdueDate"),
            lit(None).cast("date").alias("DefaultDate"),
            fsad_debts.DEBT_GENERIC_STATUS_04_DATE.cast("date").alias("DenouncedDate"),
            fs_cases_snap.CAEX_CUSTOM_TEXT_30.alias("MasterExposureKey"),
            target_table_qualco_ab.SlaveFlag.cast("boolean").alias("SlaveFlag"),
            dim_cases_static.CASE_PERIODICITY.cast("int").alias("InstalmentFrequency"),
            fs_cases_snap.CASE_CUSTOM_DECIMAL_01.cast("decimal(19,2)").alias("InstallmentFixedAmount"), #new
            lit(None).cast("int").alias("NumberOfInstallments"),
            lit(None).alias("FirstInstallmentDate"),
            fs_cases_snap.CASE_BILL_DATE.cast("date").alias("NextInstallmentDate"),
            fs_cases_snap.CASE_CUSTOM_DECIMAL_01.alias("NextInstallmentAmount"),
            lit(None).cast("date").alias("PreviousInstallmentDate"),
            fsad_case_billing.CASB_EOD_DATE.cast("date").alias("LastBillingDate"),
            dim_cases_static.CASE_UNP_BILL_NUM.cast("string").alias(
                "NumberOfUnpaidBills"
            ),
            lit(None).cast("int").alias("LastRestructuringTypeCode"),
            lit(None).cast("string").alias("LastRestructuringType"),
            lit(None).cast("date").alias("LastRestructuringDate"),
            lit(None).cast("date").alias("RestructuringPEEDate"),
            lit(None).alias("RestructuringPEETypeCode"),
            lit(None).alias("RestructuringPEEType"),
            fs_cases_snap.CASE_OBJECTIVE_STATUS.cast("string").alias(
                "ObjectiveStatusCode"
            ),
            # lit(None).cast("string").alias("ObjectiveStatus"),
            fs_cases_snap.CASE_OBJECTIVE_STATUS.cast("string").alias(
                "ObjectiveStatus"
            ),  #REPBTC-90 KS Change 10/06/2025 
            dim_cases_static.CASE_GENERIC_STATUS_03_DATE.cast("date").alias(
                "ObjectiveStatusDate"
            ),
            lit(None).cast("date").alias("GracePeriodEndDate"),
            lit(None).cast("string").alias("GracePeriodTypeCode"),
            lit(None).cast("string").alias("GracePeriodType"),
            lit(None).cast("string").alias("GracePeriodInterestPayableFlag"),
            lit(None).cast("timestamp").alias("GracePeriodNextInterestDate"),
            lit(None).cast("decimal(3,0)").alias("GracePeriodInterestFrequency"),
            fs_cases_snap.CSTM_DECIMAL_07.cast("decimal(19,2)").alias("OffBalance"),
            fs_cases_snap.CSTM_DECIMAL_06.cast("decimal(19,2)").alias("OnBalance"),
            fs_cases_snap.CASE_OUTST_BALANCE.cast("string").alias(
                "OutstandingTotalBalance"
            ),
            fsad_debts.DEBT_CAPITAL_AMT.cast("decimal(19,2)").alias("Principal"),
            lit(None).cast("decimal(19,2)").alias("Interest"),
            lit(None).cast("decimal(19,2)").alias("UnrealizedInterest"),
            lit(None).cast("decimal(19,2)").alias("Commission"),
            fs_cases_snap.CAEX_CUSTOM_DECIMAL_09.cast("decimal(19,2)").alias("Expense"),
            lit(None).cast("decimal(19,2)").alias("Accrued"),
            fs_cases_snap.CASE_DEBT_AMOUNT.cast("decimal(26,4)").alias("DueAmount"),
            lit(None).cast("decimal(19,2)").alias("DuePrincipal"),
            lit(None).cast("decimal(19,2)").alias("DueInterest"),
            lit(None).cast("decimal(19,2)").alias("DueUnrealizedInterest"),
            lit(None).cast("decimal(19,2)").alias("DueCommission"),
            lit(None).cast("decimal(19,2)").alias("DueExpense"),
            lit(None).cast("decimal(19,2)").alias("DueAccrued"),
            lit(None).cast("decimal(19,2)").alias("PenaltyInterest"),
            lit(None).cast("decimal(19,2)").alias("PenaltyUnrealizedInterest"),
            fs_cases_snap.CAEX_CUSTOM_DECIMAL_07.cast("decimal(19,6)").alias(
                "InterestRate"
            ),
            lit(None).cast("int").alias("InterestCode"),
            lit(None).alias("InterestDescription"),
            fsad_cases_extra_open.CAEX_CUSTOM_DECIMAL_28.cast("decimal(19,6)").alias(
                "BaseRate"
            ),
            lit(None).alias("BaseCode"),
            lit(None).alias("BaseDescription"),
            lit(None).alias("N128Rate"),
            fsad_cases_extra_open.CAEX_CUSTOM_DECIMAL_29.cast("decimal(19,6)").alias(
                "SpreadRate"
            ),
            fs_cases_snap.CAEX_CUSTOM_DECIMAL_12.cast("decimal(19,6)").alias(
                "PenaltyRate"
            ),
            lit(None).alias("InterestRateTypeCode"),
            lit(None).alias("InterestRateType"),
            lit(None).alias("SpreadCurrencyCode"),
            lit(None).alias("SpreadCurrency"),
            lit(None).alias("SpreadPeriod"),
            lit(None).alias("InterestDuration"),
            lit(None).alias("DayCounts"),
            lit(None).alias("InterestFrequency"),
            lit(None).alias("InterestFixedDate"),
            lit(None).alias("InterestValidityDate"),
            lit(None).alias("InterestChangeStartDate"),
            lit(None).alias("InterestChangeEndDate"),
            lit(None).alias("InterestRateUpdateDate"),
            lit(None).alias("InterestCalcStopFlag"),
            lit(None).alias("AutoRenewalFlag"),
            dim_cases_static.CASE_CURRENTLY_OPEN.cast("boolean").alias("IsActiveFlag"),
            fs_cases_snap.CASE_WR_OF_AMOUNT.alias("WriteOffAmount"),
            fs_cases_snap.CASE_WR_OF_DATE.cast("date").alias("WriteOffDate"),
            dim_cases_static.CASE_BU.cast("string").alias("AssetSegmentCode"),
            dim_cases_static.CASE_BU.cast("string").alias("AssetSegment"),

            lit(None).cast("string").alias("DiasNumber"),
            fsad_cases_extra_open.CAEX_CUSTOM_DECIMAL_30.cast("string").alias(
                "ProvisionFrozenInterestAmount"
            ),
            dim_cases_static.CAEX_CUSTOM_TEXT_22.cast("string").alias(
                "AccountNticeBUKey"
            ),
            fsad_cases_extra_open.CAEX_CUSTOM_TEXT_08.cast("string").alias("SayFlag"),
            dim_cases_static.CAEX_CUSTOM_TEXT_22.cast("string").alias(
                "InsuranceCompany"
            ),
            dim_cases_static.CASE_APPLIC_NUM.cast("string").alias("PhysicalNumber"),
            fs_cases_snap.CAEX_CUSTOM_DATETIME_12.cast("string").alias("PreCourtDate"),
            fs_cases_snap.CAEX_CUSTOM_DATETIME_11.cast("string").alias("CourtDate"),
            fs_cases_snap.CAEX_CUSTOM_DATETIME_03.cast("string").alias(
                "ContractSigningDate"
            ),
            dim_cases_static.CAEX_CUSTOM_TEXT_16.cast("string").alias(
                "ExposureLimitAmount"
            ),
            lit(None).alias("BaloonAmount"),
            fsad_debts.DEBT_ADMINISTRATION_STATUS_CODE.cast("int").alias("AdministrationStatusCode"), #new
            fsad_debts.DEBT_ADMINISTRATION_STATUS_CODE.cast("int").alias("AdministrationStatus"), #new
            dim_cases_static.CAEX_CUSTOM_TEXT_16.cast("string").alias("Tenor"),
            dim_cases_static.CASE_SOLD_FLAG.alias("IsSoldFlag"),
            fs_cases_snap.CAEX_CUSTOM_TEXT_44.cast("string").alias("IsSubsidyFlag"),
            fs_cases_snap_extra.CEXT_GENERIC_BIT_02.alias("IsGuaranteeFlag"),
            fs_cases_snap.CAEX_CUSTOM_INT_14.cast("string").alias("EverDenouncedFlag"),
            fs_cases_snap_extra.CEXT_GENERIC_DATETIME_04.cast("string").alias(
                "EverDenouncedDate"
            ),
            fs_cases_snap.CSTM_TEXT_10.cast("string").alias("SPVCode"),
            fsad_list_of_values.LOV_COMMENTS.cast("string").alias("SPV"),
            lit(None).alias("TotalOverdueInsterest"),
            lit(None).alias("TotalOverdueInstallments"),
            fsad_cases_extra_open.CAEX_CUSTOM_DECIMAL_34_LCY.cast(
                "decimal(19,4)"
            ).alias("OutstandingTotalBalanceMLT"),
            fs_cases_snap_extra.CEXT_GENERIC_BIT_03.cast("boolean").alias(
                "AllBorrowersDeceasedFlag"
            ),
            fs_cases_snap.BRN_CODE_TREATMENT.alias("TreatmentBranchCode"),
            dim_branch_treatment.BRN_DESC_treatment.alias("TreatmentBranch"),
            target_table_qualco_ab.LetterStatusCode.cast("string").alias(
                "LetterStatusCode"
            ),
            target_table_qualco_ab.LetterStatusCode.cast("string").alias(
                "LetterStatus"
            ),
            target_table_qualco_ab.LetterStatusDate.cast("Date").alias(
                "LetterStatusDate"
            ),
            target_table_qualco_ab.LOV_CODE_2.cast("string").alias("CoCStatusCode"),
            target_table_qualco_ab.LOV_DESC_2.cast("string").alias("CoCStatus"),
            fs_cases_snap.CASP_GENERIC_INT_05_CODE_DATE.cast("Date").alias("CoCAccountStatusDate"), #new
            target_table_qualco_ab.FirstLetterDate.cast("Date").alias(
                "FirstLetterDate"
            ),
            target_table_qualco_ab.SecondLetterDate.cast("Date").alias(
                "SecondLetterDate"
            ),
            target_table_qualco_ab.ThirdLetterDate.cast("Date").alias(
                "ThirdLetterDate"
            ),
            target_table_qualco_ab.FirstLetterReminderDate.cast("Date").alias(
                "FirstLetterReminderDate"
            ),
            fs_cases_snap.CAEX_CUSTOM_BIT_07.cast("Boolean").alias("FirstLetterFlag"), #new
            fs_cases_snap.CAEX_CUSTOM_BIT_11.cast("Boolean").alias("FirstLetterReminderFlag"), #new
            fs_cases_snap.CAEX_CUSTOM_BIT_08.cast("Boolean").alias("SecondLetterFlag"), #new
            fs_cases_snap.CAEX_CUSTOM_BIT_09.cast("Boolean").alias("ThirdLetterFlag"),#new
            fs_cases_snap.CAEX_CUSTOM_DATETIME_31.cast("Date").alias("ExclusionDate"),#new
            fs_cases_snap.CAEX_CUSTOM_INT_19.cast("String").alias("ExclusionReasonCode"),#new
            fs_cases_snap.CAEX_CUSTOM_INT_19.cast("String").alias("ExclusionReason"),#new
            at_cases_extension.cext_dedicated_property_08_nvarchar.alias("LegalActionPriority"),
            fsad_cases_extra_open.CAEX_CUSTOM_DECIMAL_66_LCY.cast("decimal(19,4)").alias("DueDebtAmount"),
            lit(None).alias("JustificationCode"),
            lit(None).alias("JustificationComments"),
            dim_cases_static.businessdate.cast("date").alias("BusinessDate"),
            target_table_qualco_ab.ForcedOnboarding.cast("Boolean"),
            target_table_qualco_ab.HolisticFlag.cast("Boolean"),
            target_table_qualco_ab.ForcedOnboardReason.cast("String"),
            lit(None).cast("decimal(15,2)").alias("url_nrm_int_amn"),
        )

        # ========================== Append ==========================

        result_df_qualco_ab.write.format("delta").clusterBy("BusinessDate").mode(
            "append"
        ).saveAsTable(curated_table_path)

    else:
        null_args = [
            arg_name
            for arg_name, arg in args
            if arg
                in [
                curated_zone,
                catalog_landed,
                reference_zone,
                run_date,
                schema,
                env_prefix,
            ]

            and arg is None
        ]

        for arg_name in null_args:
            logging.error(f"No {arg_name} specified!")

    logging.info("We did it!")

# COMMAND ----------

load_curated_exposure_qca(spark, curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix)
