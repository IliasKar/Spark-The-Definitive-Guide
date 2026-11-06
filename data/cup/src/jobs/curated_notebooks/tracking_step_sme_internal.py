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

class TrackingStepSMEInternal:
    
    def __init__(self, spark: SparkSession, catalog_landed: str, run_date: str):
        self.spark = spark
        self.catalog_landed = catalog_landed
        self.run_date = run_date
        self.date_filter = f"businessdate = {self.run_date!r}"

    def trackingStep(self) -> SparkDataFrame:

        # ============================== cepalplatform_smedebtorstatusrecollateralaction ====================================

        debtor_status_recollateral_action_extra_filters = None
        if debtor_status_recollateral_action_extra_filters is not None:
            debtor_status_recollateral_action_extra_filters.append(self.date_filter)
            debtor_status_recollateral_action_final_filters = " AND ".join(
                debtor_status_recollateral_action_extra_filters
                )
        else:
            debtor_status_recollateral_action_final_filters = self.date_filter

        debtor_status_recollateral_action_selected_filters = [
            "DebtorStatusReColActionID",
            "DebtorStatusDataID",
            "DebtorStatusReColID",
            "DebtorStatusReColActionStartDate",
            "DebtorStatusReColActionTargetEndDate",
            "DebtorStatusReColActionIsPrimary",
            "DebtorStatusReColActionStrategyID",
            "DebtorStatusReColActionStepID",
            "DebtorStatusReColActionStepStatusID",
            "DebtorStatusReColActionComments",
            "DebtorStatusReColActionIsDeleted",
            "DebtorStatusReColActionCreationDate",
            "DebtorStatusReColActionCreationUser",
            "DebtorStatusReColActionLastModifiedDate",
            "DebtorStatusReColActionLastModifiedUser",
            "DebtorStatusReColActionRecoveryAmount",
            "DebtorStatusReColActionExpenseAmount",
            "DebtorStatusReColActionNumOfPastAuctions",
            "DebtorStatusReColActionLastAuctionDate",
            "DebtorStatusReColActionNextAuctionDate",
            "DebtorStatusReColActionNextAuctionPrice",
            "BusinessDate",
            ]

        debtor_status_recollateral_action = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smedebtorstatusrecollateralaction",
            debtor_status_recollateral_action_final_filters,
            debtor_status_recollateral_action_selected_filters,
            )

        # ============================== cepalplatform_smedebtorstatusrecollateral  ====================================

        debtor_status_recollateral_extra_filters = None
        if debtor_status_recollateral_extra_filters is not None:
            debtor_status_recollateral_extra_filters.append(self.date_filter)
            debtor_status_recollateral_final_filters = " AND ".join(
                debtor_status_recollateral_extra_filters
                )
        else:
            debtor_status_recollateral_final_filters = self.date_filter

        debtor_status_recollateral_selected_filters = [
            "DebtorStatusReColID",
            "DebtorStatusReColPropertyKey",
            "DebtorStatusReColPropertyCategory",
            "DebtorStatusReColPropertySubCategory",
            "DebtorStatusReColPropertyUsage",
            "DebtorStatusReColPropertyLandSquareMetre",
            "DebtorStatusReColPropertySquareMetre",
            "DebtorStatusReColPropertyAddress",
            "DebtorStatusReColPropertyArea",
            "DebtorStatusReColPropertyCity",
            "DebtorStatusReColPropertyZipCode",
            ]

        debtor_status_recollateral = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smedebtorstatusrecollateral",
            debtor_status_recollateral_final_filters,
            debtor_status_recollateral_selected_filters,
            )

        # ============================== cepalplatform_smedebtorstatusdata  ====================================

        debtor_status_extra_filters = None
        if debtor_status_extra_filters is not None:
            debtor_status_extra_filters.append(self.date_filter)
            debtor_status_final_filters = " AND ".join(
                debtor_status_extra_filters
                )
        else:
            debtor_status_final_filters = self.date_filter

        debtor_status_selected_filters = [
            "DebtorStatusDataID",
            "DebtorStatusDataPrfID",
            "DebtorStatusDataPrfCDI",
            "DebtorStatusDataPrfAFM",
            "DebtorStatusDataPrfGroupID",
            "DebtorStatusDataPrfGroupDescription",
            "DebtorStatusDataPrfOriginalBankGroupID",
            "DebtorStatusDataPrfsCustomerName",
            "DebtorStatusDataPrfTotalBalance",
            "DebtorStatusDataPrfCurrencyCode",
            "DebtorStatusDataStatusID",
            "DebtorStatusDataStatusComments",
            "DebtorStatusDataNextActionID",
            "DebtorStatusDataNextActionComments",
            "DebtorStatusDataLegalStatusID",
            "DebtorStatusDataLegalOpinionID",
            "DebtorStatusDataLegalComments",
            "DebtorStatusDataAgreementNumber",
            "DebtorStatusDataInitialStrategyID",
            "DebtorStatusDataCurrentStrategyID",
            "DebtorStatusDataCurrentStrategyComments",
            "DebtorStatusDataLastLegalActionType",
            "DebtorStatusDataLastLegalActionDate",
            "DebtorStatusDataAssignedAgent",
            "DebtorStatusDataAssignedAgentID",
            "DebtorStatusDataLastModifiedDate",
            "DebtorStatusDataModifiedByUser",
            "DebtorStatusDataSegment",
            "DebtorStatusDataBalanceCategory",
            "PortfolioID",
            ]

        debtor_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smedebtorstatusdata",
            debtor_status_final_filters,
            debtor_status_selected_filters,
            )

        # ============================== cepalplatform_smerfdtdsrecollateralstrategy  ====================================

        recollateral_strategy_extra_filters = None
        if recollateral_strategy_extra_filters is not None:
            recollateral_strategy_extra_filters.append(self.date_filter)
            recollateral_strategy_final_filters = " AND ".join(
                recollateral_strategy_extra_filters
                )
        else:
            recollateral_strategy_final_filters = self.date_filter

        recollateral_strategy_selected_filters = ["ID", "Label"]
        recollateral_strategy = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtdsrecollateralstrategy",
            recollateral_strategy_final_filters,
            recollateral_strategy_selected_filters,
            )

        # ============================== cepalplatform_smerfdtdsrecollateralstep  ====================================

        recollateral_step_extra_filters = None
        if recollateral_step_extra_filters is not None:
            recollateral_step_extra_filters.append(self.date_filter)
            recollateral_step_final_filters = " AND ".join(
                recollateral_step_extra_filters
                )
        else:
            recollateral_step_final_filters = self.date_filter

        recollateral_step_selected_filters = ["StrategyID", "ID", "Label", "Code"]
        recollateral_step = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtdsrecollateralstep ",
            recollateral_step_final_filters,
            recollateral_step_selected_filters,
            )

        # ============================== cepalplatform_smerfdtdsrecollateralstepstatus  ====================================

        recollateral_step_status_extra_filters = None
        if recollateral_step_status_extra_filters is not None:
            recollateral_step_status_extra_filters.append(self.date_filter)
            recollateral_step_status_final_filters = " AND ".join(
                recollateral_step_status_extra_filters
                )
        else:
            recollateral_step_status_final_filters = self.date_filter

        recollateral_step_status_selected_filters = ["ID", "Label"]
        recollateral_step_status = load_landed_delta_table(
            self.spark,
            self.catalog_landed,  
            "cepalplatform_smerfdtdsrecollateralstepstatus ",
            recollateral_step_status_final_filters,
            recollateral_step_status_selected_filters,
            )

        # ============================== Joins  ====================================

        trackingstep = (
            debtor_status_recollateral_action
            .join(debtor_status_recollateral, debtor_status_recollateral.DebtorStatusReColID == debtor_status_recollateral_action.DebtorStatusReColID)
            .join(debtor_status, debtor_status.DebtorStatusDataID == debtor_status_recollateral_action.DebtorStatusDataID)
            .join(recollateral_strategy, recollateral_strategy.ID == debtor_status_recollateral_action.DebtorStatusReColActionStrategyID)
            .join(recollateral_step, 
                (recollateral_step.StrategyID == recollateral_strategy.ID)
                & (recollateral_step.ID == debtor_status_recollateral_action.DebtorStatusReColActionStepID))
            .join(recollateral_step_status, recollateral_step_status.ID == debtor_status_recollateral_action.DebtorStatusReColActionStepStatusID)
        )

        # ============================== Final table  ====================================

        trackingstep_target = trackingstep.select(
            debtor_status_recollateral_action.DebtorStatusReColActionID,
            debtor_status_recollateral_action.DebtorStatusDataID,
            debtor_status_recollateral_action.DebtorStatusReColID,
            debtor_status_recollateral_action.DebtorStatusReColActionStartDate,
            debtor_status_recollateral_action.DebtorStatusReColActionTargetEndDate,
            debtor_status_recollateral_action.DebtorStatusReColActionIsPrimary,
            debtor_status_recollateral_action.DebtorStatusReColActionStrategyID,
            debtor_status_recollateral_action.DebtorStatusReColActionStepID,
            debtor_status_recollateral_action.DebtorStatusReColActionStepStatusID,
            debtor_status_recollateral_action.DebtorStatusReColActionComments,
            debtor_status_recollateral_action.DebtorStatusReColActionIsDeleted,
            debtor_status_recollateral_action.DebtorStatusReColActionCreationDate,
            debtor_status_recollateral_action.DebtorStatusReColActionCreationUser,
            debtor_status_recollateral_action.DebtorStatusReColActionLastModifiedDate,
            debtor_status_recollateral_action.DebtorStatusReColActionLastModifiedUser,
            debtor_status_recollateral_action.DebtorStatusReColActionRecoveryAmount,
            debtor_status_recollateral_action.DebtorStatusReColActionExpenseAmount,
            debtor_status_recollateral_action.DebtorStatusReColActionNumOfPastAuctions,
            debtor_status_recollateral_action.DebtorStatusReColActionLastAuctionDate,
            debtor_status_recollateral_action.DebtorStatusReColActionNextAuctionDate,
            debtor_status_recollateral_action.DebtorStatusReColActionNextAuctionPrice,
            debtor_status_recollateral.DebtorStatusReColPropertyKey,
            debtor_status_recollateral.DebtorStatusReColPropertyCategory,
            debtor_status_recollateral.DebtorStatusReColPropertySubCategory,
            debtor_status_recollateral.DebtorStatusReColPropertyUsage,
            debtor_status_recollateral.DebtorStatusReColPropertyLandSquareMetre,
            debtor_status_recollateral.DebtorStatusReColPropertySquareMetre,
            debtor_status_recollateral.DebtorStatusReColPropertyAddress,
            debtor_status_recollateral.DebtorStatusReColPropertyArea,
            debtor_status_recollateral.DebtorStatusReColPropertyCity,
            debtor_status_recollateral.DebtorStatusReColPropertyZipCode,
            debtor_status.DebtorStatusDataID,
            debtor_status.DebtorStatusDataPrfID,
            debtor_status.DebtorStatusDataPrfCDI,
            debtor_status.DebtorStatusDataPrfAFM,
            debtor_status.DebtorStatusDataPrfGroupID,
            debtor_status.DebtorStatusDataPrfGroupDescription,
            debtor_status.DebtorStatusDataPrfOriginalBankGroupID,
            debtor_status.DebtorStatusDataPrfsCustomerName,
            debtor_status.DebtorStatusDataPrfTotalBalance,
            debtor_status.DebtorStatusDataPrfCurrencyCode,
            debtor_status.DebtorStatusDataStatusID,
            debtor_status.DebtorStatusDataStatusComments,
            debtor_status.DebtorStatusDataNextActionID,
            debtor_status.DebtorStatusDataNextActionComments,
            debtor_status.DebtorStatusDataLegalStatusID,
            debtor_status.DebtorStatusDataLegalOpinionID,
            debtor_status.DebtorStatusDataLegalComments,
            debtor_status.DebtorStatusDataAgreementNumber,
            debtor_status.DebtorStatusDataInitialStrategyID,
            debtor_status.DebtorStatusDataCurrentStrategyID,
            debtor_status.DebtorStatusDataCurrentStrategyComments,
            debtor_status.DebtorStatusDataLastLegalActionType,
            debtor_status.DebtorStatusDataLastLegalActionDate,
            debtor_status.DebtorStatusDataAssignedAgent,
            debtor_status.DebtorStatusDataAssignedAgentID,
            debtor_status.DebtorStatusDataLastModifiedDate,
            debtor_status.DebtorStatusDataModifiedByUser,
            debtor_status.DebtorStatusDataSegment,
            debtor_status.DebtorStatusDataBalanceCategory,
            debtor_status.PortfolioID,
            recollateral_step.Label.alias("strategy"),
            recollateral_strategy.Label.alias("Step"),
            recollateral_step.Code.alias("Step_id"),
            recollateral_step_status.Label.alias("Step_status"),
            debtor_status_recollateral_action.BusinessDate,
        )

        return trackingstep_target
