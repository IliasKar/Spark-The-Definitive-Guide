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
    lit,
    sha2,
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

def load_curated_auction_extfile(
    spark, curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix
) -> None:
    args = [curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix]
    if all(item is not None for item in args) :
        date_filter = f"businessdate = {run_date!r}"

        # schema = (
        #     "AuctionID  string"
        #     ",SourceSystem string"
        #     ",IngestionDate timestamp"
        #     ",BatchId string"
        #     ",AuctionKey    string"
        #     ",RelatedAuctionKey  string"
        #     ",OriginAuctionKey string"
        #     ",ConductionDate    date"
        #     ",PublishedDate date"
        #     ",FirstOfferPrice   decimal(18,2)"
        #     ",HastenerCode  string"
        #     ",Hastener  string"
        #     ",HastenerTaxRegistrationNumber   string"
        #     ",HastenerCountry   string"
        #     ",AuctionStatusCode string"
        #     ",AuctionStatus string"
        #     ",DebtorPartyKey string"
        #     ",DebtorTaxRegistrationNumber   string"
        #     ",DebtorDescription    string"
        #     ",CourtType string"
        #     ",AuctionType   string"
        #     ",MultipleDeptorsFlag   boolean"
        #     ",Servicer  string"
        #     ",HasJudgmentFlag   boolean"
        #     ",FileContent   string"
        #     ",BusinessDate date"
        # )
        cluster_columns = ["BusinessDate"]

        curated_table_path = f"{curated_zone}.Auction"

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
                    "extfile",
                )        
        else:
            logging.info("Tables doe not exist, no need to delete entries for business date!")

        # ================== basic_data ==================
        basic_data_extra_filters = None

        if basic_data_extra_filters is not None:
            basic_data_extra_filters.append(date_filter)
            basic_data_final_filters = " AND ".join(basic_data_extra_filters)
        else:
            basic_data_final_filters = date_filter

            basic_data_selected_fields = [
                "UniqueCode",
                "RelatedAuctionCode",
                "ConductionDate",
                "PublishDate",
                "InitialPrice",
                "HastenerDescr",
                "HastenerVAT",
                "HastenerCountry",
                "Status",
                "DebtorVatNumber",
                "DebtorDescr",
                "Court_Type",
                "Type",
                "MultipleDeptors",
                "Servicer",
                "HasJudgment",
                "businessdate",
            ]

            basic_data = load_landed_delta_table(
                spark,
                catalog_landed,  # type: ignore
                "newsphone_basic_data",
                basic_data_final_filters,
                basic_data_selected_fields,
            )
        # ================== auction_report_content ==================

        # auction_report_content_extra_filters = None

        # if auction_report_content_extra_filters is not None:
        #     auction_report_content_extra_filters.append(date_filter)
        #     auction_report_content_final_filters = " AND ".join(
        #         auction_report_content_extra_filters
        #     )
        # else:
        #     auction_report_content_final_filters = date_filter

        # auction_report_content_selected_fields = ["FileContent", "UniqueCode"]

        # auction_report_content = load_landed_delta_table(
        #     spark,
        #     catalog_landed,  # type: ignore
        #     "newsphone_auction_report_content",
        #     auction_report_content_final_filters,
        #     auction_report_content_selected_fields,
        # )
        # auction_report_content = auction_report_content.withColumnRenamed(
        #     "UniqueCode", "UniqueCode_arc"
        # )

        # ================== auction_extfile_df ==================
        auction_extfile_primary_key_fields = [
            basic_data.UniqueCode,
            lit("extfile"),
        ]

        # auction_extfile_df = basic_data.join(
        #     auction_report_content,
        #     (basic_data["UniqueCode"] == auction_report_content["UniqueCode_arc"]),
        #     "left",
        # )

        auction_extfile_df = (
            basic_data.withColumn(
                "AuctionID",
                sha2(concat_ws(";", *auction_extfile_primary_key_fields), 256),
            )
            .withColumn("SourceSystem", lit("extfile").cast("string"))
            .withColumn("BatchId", lit(None).cast("string"))
            .withColumn("HastenerCode", lit(None).cast("string"))
            .withColumn("AuctionStatus", lit(None).cast("string"))
            .withColumn("DebtorPartyKey", lit(None).cast("string"))
        )

        auction_extfile_df = auction_extfile_df.select(
            auction_extfile_df.AuctionID.cast("string"),
            auction_extfile_df.SourceSystem,
            auction_extfile_df.BatchId,
            lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")).alias(
                "IngestionTimestamp"
            ),
            auction_extfile_df.HastenerCode,
            auction_extfile_df.AuctionStatus,
            auction_extfile_df.DebtorPartyKey,
            auction_extfile_df.UniqueCode.cast("string").alias("AuctionKey"),
            auction_extfile_df.RelatedAuctionCode.alias("RelatedAuctionKey"),
            auction_extfile_df.ConductionDate.alias("ConductionDate"),
            auction_extfile_df.PublishDate.alias("PublishedDate"),
            auction_extfile_df.InitialPrice.alias("FirstOfferPrice"),
            auction_extfile_df.HastenerDescr.alias("Hastener"),
            auction_extfile_df.HastenerVAT.alias("HastenerTaxRegistrationNumber"),
            auction_extfile_df.HastenerCountry.alias("HastenerCountry"),
            auction_extfile_df.Status.alias("AuctionStatusCode"),
            auction_extfile_df.DebtorVatNumber.alias("DebtorTaxRegistrationNumber"),
            auction_extfile_df.DebtorDescr.alias("DebtorDescription"),
            auction_extfile_df.Court_Type.alias("CourtType"),
            auction_extfile_df.Type.alias("AuctionType"),
            auction_extfile_df.MultipleDeptors.alias("MultipleDeptorsFlag"),
            auction_extfile_df.Servicer.alias("Servicer"),
            auction_extfile_df.HasJudgment.alias("HasJudgmentFlag"),
            # auction_extfile_df.FileContent.alias("FileContent"),
            auction_extfile_df.businessdate.alias("BusinessDate"),
        )

        df = auction_extfile_df

        mapping = df.select(
            col("AuctionKey").alias("child"), col("RelatedAuctionKey").alias("parent")
        )

        # Iteratively update parent values
        while True:
            updated_mapping = (
                mapping.alias("m")
                .join(
                    mapping.alias("p"), col("m.parent") == col("p.child"), "left_outer"
                )
                .select(
                    col("m.child"),
                    coalesce(col("p.parent"), col("m.parent")).alias("parent"),
                )
            )

            # If mapping is not updated, break the loop
            if updated_mapping.subtract(mapping).count() == 0:
                break

            mapping = updated_mapping

        # Join the final mapping with the original DataFrame and update RelatedAuctionKey values
        df = (
            df.join(mapping, df.AuctionKey == mapping.child)
            .select(
                df.AuctionID.cast("string"),
                df.SourceSystem,
                df.BatchId,
                df.IngestionTimestamp,
                df.RelatedAuctionKey,
                df.HastenerCode,
                df.AuctionStatus,
                df.DebtorPartyKey,
                df.AuctionKey,
                col("parent").alias("OriginAuctionKey"),
                df.ConductionDate,
                df.PublishedDate,
                df.FirstOfferPrice,
                df.Hastener,
                df.HastenerTaxRegistrationNumber,
                df.HastenerCountry,
                df.AuctionStatusCode,
                df.DebtorTaxRegistrationNumber,
                df.DebtorDescription,
                df.CourtType,
                df.AuctionType,
                df.MultipleDeptorsFlag,
                df.Servicer,
                df.HasJudgmentFlag,
                # df.FileContent,
                df.BusinessDate,
            )
            .distinct()
        )

        df = df.withColumn(
            "OriginAuctionKey",
            when(col("OriginAuctionKey").isNull(), col("AuctionKey")).otherwise(
                col("OriginAuctionKey")
            ),
        )

        # ================== Append ==================

        df.write.format("delta").clusterBy("BusinessDate").mode("append").saveAsTable(
            curated_table_path
        )
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

load_curated_auction_extfile(spark, curated_zone, catalog_landed, reference_zone, run_date, schema, env_prefix)
