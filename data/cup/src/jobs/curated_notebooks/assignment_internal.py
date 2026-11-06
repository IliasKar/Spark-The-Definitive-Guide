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
    current_date,
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


class AssignmentInternal:

    def __init__(self, spark: SparkSession, catalog_landed: str, run_date: str, env: str):
        self.spark = spark
        self.catalog_landed = catalog_landed
        self.run_date = run_date
        self.env = env
        self.date_filter = f"businessdate = {self.run_date!r}"
        # Extract catalog prefix from catalog_landed (e.g. "`d-cdh`.`landed`" -> "`d-cdh`")
        self.catalog = catalog_landed.split(".")[0].strip()  # e.g. "`d-cdh`"

    # ============================== qc_case_master ====================================

    def qcCaseMaster(self) -> SparkDataFrame:

        case_master_extra_filters = None
        if case_master_extra_filters is not None:
            case_master_extra_filters.append(self.date_filter)
            case_master_final_filters = " AND ".join(case_master_extra_filters)
        else:
            case_master_final_filters = self.date_filter

        case_master_selected_fields = [
            "vend_code",
            "Extra_Segment",
            "extc_code",
            "Sub_Segment",
            "debt_code",
            "case_code",
            "que_desc",
            "process_type",
            "emp_code_assignment",
            "cust_code",
            "strategy_step_id",
            "case_relation_type_code",
            "case_currently_open_flag",
        ]

        case_master = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_case_master",
            case_master_final_filters,
            case_master_selected_fields,
        )

        return case_master

    # ============================== qc_customer ====================================

    def qcCustomer(self) -> SparkDataFrame:

        customer_extra_filters = None
        if customer_extra_filters is not None:
            customer_extra_filters.append(self.date_filter)
            customer_final_filters = " AND ".join(customer_extra_filters)
        else:
            customer_final_filters = self.date_filter

        customer_selected_fields = [
            "Segment",
            "channel",
            "cust_code",
        ]

        customer = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_customer",
            customer_final_filters,
            customer_selected_fields,
        )

        return customer

    # ============================== qc_employees ====================================

    def qcEmployees(self) -> SparkDataFrame:

        employees_extra_filters = None
        if employees_extra_filters is not None:
            employees_extra_filters.append(self.date_filter)
            employees_final_filters = " AND ".join(employees_extra_filters)
        else:
            employees_final_filters = self.date_filter

        employees_selected_fields = [
            "emp_code",
            "team_id",
            "dep_code",
            "team_name",
        ]

        employees = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_employees",
            employees_final_filters,
            employees_selected_fields,
        )

        return employees

    # ============================== qc_strategy_step ====================================

    def qcStrategyStep(self) -> SparkDataFrame:

        strategy_step_extra_filters = None
        if strategy_step_extra_filters is not None:
            strategy_step_extra_filters.append(self.date_filter)
            strategy_step_final_filters = " AND ".join(strategy_step_extra_filters)
        else:
            strategy_step_final_filters = self.date_filter

        strategy_step_selected_fields = [
            "strategy_step_id",
        ]

        strategy_step = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_strategy_step",
            strategy_step_final_filters,
            strategy_step_selected_fields,
        )

        return strategy_step

    # ============================== qc_external_companies ====================================

    def qcExternalCompanies(self) -> SparkDataFrame:

        external_companies_extra_filters = None
        if external_companies_extra_filters is not None:
            external_companies_extra_filters.append(self.date_filter)
            external_companies_final_filters = " AND ".join(external_companies_extra_filters)
        else:
            external_companies_final_filters = self.date_filter

        external_companies_selected_fields = [
            "EXTC_COMMENT",
            "extc_code",
        ]

        external_companies = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_external_companies",
            external_companies_final_filters,
            external_companies_selected_fields,
        )

        return external_companies

    # ============================== params_assignment_queue_setup ====================================

    def qcAssignmentQueueSetup(self) -> SparkDataFrame:

        # The reference table was imported with the first data row as column headers.
        # Actual columns: "Almost Paid Off"=assignment, "Fulfilled Settlment / Paid Off"=assignment_mapping,
        # "Mercury almost Paid Off"=queue_desc, "6"=vendor_code
        # The original header row was: assignment, assignment_mapping, queue_desc, vendor_code
        # so we just rename the columns - no need to add back the header as a data row.
        df = self.spark.read.table(f"{self.catalog}.`reference`.params_assignment_queue_setup")

        # Rename columns to proper names
        df = df.toDF("assignment", "assignment_mapping", "queue_desc", "vendor_code")

        df = df.select("assignment_mapping", "queue_desc")

        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = df.withColumn(col_name, trim(df[col_name]))

        return df

    # ============================== qc_team ====================================

    def qcTeam(self) -> SparkDataFrame:

        team_extra_filters = None
        if team_extra_filters is not None:
            team_extra_filters.append(self.date_filter)
            team_final_filters = " AND ".join(team_extra_filters)
        else:
            team_final_filters = self.date_filter

        team_selected_fields = [
            "TEA_NAME",
            "TEA_CODE",
            "channel_id",
        ]

        team = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_team",
            team_final_filters,
            team_selected_fields,
        )

        return team

    # ============================== qc_case_external_assignment ====================================

    def qcCaseExternalAssignment(self) -> SparkDataFrame:

        case_external_assignment_extra_filters = None
        if case_external_assignment_extra_filters is not None:
            case_external_assignment_extra_filters.append(self.date_filter)
            case_external_assignment_final_filters = " AND ".join(case_external_assignment_extra_filters)
        else:
            case_external_assignment_final_filters = self.date_filter

        case_external_assignment_selected_fields = [
            "CASE_CODE",
            "VEND_CODE",
            "PACK_ASSIGN_DATE",
        ]

        case_external_assignment = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_case_external_assignment",
            case_external_assignment_final_filters,
            case_external_assignment_selected_fields,
        )

        return case_external_assignment

    # ============================== qc_department ====================================

    def qcDepartment(self) -> SparkDataFrame:

        department_extra_filters = None
        if department_extra_filters is not None:
            department_extra_filters.append(self.date_filter)
            department_final_filters = " AND ".join(department_extra_filters)
        else:
            department_final_filters = self.date_filter

        department_selected_fields = [
            "dep_code",
            "dep_desc",
        ]

        department = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qc_department",
            department_final_filters,
            department_selected_fields,
        )

        return department

    # ============================== qca_fs_cases_snap ====================================

    def qcaFsCasesSnap(self) -> SparkDataFrame:

        fs_cases_snap_extra_filters = None
        if fs_cases_snap_extra_filters is not None:
            fs_cases_snap_extra_filters.append(self.date_filter)
            fs_cases_snap_final_filters = " AND ".join(fs_cases_snap_extra_filters)
        else:
            fs_cases_snap_final_filters = self.date_filter

        fs_cases_snap_selected_fields = [
            "CASE_CODE",
            "CUST_CODE",
            "EXTC_CODE_PRIMARY",
            "CASE_TAG",
            "QUE_CODE",
            "EMP_TAG",
            "SPET_CODE",
            "PACK_CODE",
            "PACKH3_ID",
        ]

        fs_cases_snap = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_fs_cases_snap",
            fs_cases_snap_final_filters,
            fs_cases_snap_selected_fields,
        )

        return fs_cases_snap

    # ============================== qca_dim_cases_static ====================================

    def qcaDimCasesStatic(self) -> SparkDataFrame:

        dim_cases_static_extra_filters = None
        if dim_cases_static_extra_filters is not None:
            dim_cases_static_extra_filters.append(self.date_filter)
            dim_cases_static_final_filters = " AND ".join(dim_cases_static_extra_filters)
        else:
            dim_cases_static_final_filters = self.date_filter

        dim_cases_static_selected_fields = [
            "CASE_CODE",
            "CASE_ACCOUNT_FOR_SEARCH",
            "CASE_CODE_PRIMARY",
            "CASE_CURRENTLY_OPEN",
        ]

        dim_cases_static = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_cases_static",
            dim_cases_static_final_filters,
            dim_cases_static_selected_fields,
        )

        return dim_cases_static

    # ============================== qca_dim_external_company ====================================

    def qcaDimExternalCompany(self) -> SparkDataFrame:

        dim_external_company_extra_filters = None
        if dim_external_company_extra_filters is not None:
            dim_external_company_extra_filters.append(self.date_filter)
            dim_external_company_final_filters = " AND ".join(dim_external_company_extra_filters)
        else:
            dim_external_company_final_filters = self.date_filter

        dim_external_company_selected_fields = [
            "EXTC_ID",
            "EXTC_NAME",
        ]

        dim_external_company = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_external_company",
            dim_external_company_final_filters,
            dim_external_company_selected_fields,
        )

        return dim_external_company

    # ============================== qca_dim_branch ====================================

    def qcaDimBranch(self) -> SparkDataFrame:

        dim_branch_extra_filters = None
        if dim_branch_extra_filters is not None:
            dim_branch_extra_filters.append(self.date_filter)
            dim_branch_final_filters = " AND ".join(dim_branch_extra_filters)
        else:
            dim_branch_final_filters = self.date_filter

        dim_branch_selected_fields = [
            "BRN_ID",
            "BRN_DESC",
            "BRN_INTCODE",
        ]

        dim_branch = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_branch",
            dim_branch_final_filters,
            dim_branch_selected_fields,
        )

        return dim_branch

    # ============================== qca_dim_queue ====================================

    def qcaDimQueue(self) -> SparkDataFrame:

        dim_queue_extra_filters = None
        if dim_queue_extra_filters is not None:
            dim_queue_extra_filters.append(self.date_filter)
            dim_queue_final_filters = " AND ".join(dim_queue_extra_filters)
        else:
            dim_queue_final_filters = self.date_filter

        dim_queue_selected_fields = [
            "QUE_ID",
            "QUE_DESC",
        ]

        dim_queue = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_queue",
            dim_queue_final_filters,
            dim_queue_selected_fields,
        )

        return dim_queue

    # ============================== qca_dim_collector ====================================

    def qcaDimCollector(self) -> SparkDataFrame:

        dim_collector_extra_filters = None
        if dim_collector_extra_filters is not None:
            dim_collector_extra_filters.append(self.date_filter)
            dim_collector_final_filters = " AND ".join(dim_collector_extra_filters)
        else:
            dim_collector_final_filters = self.date_filter

        dim_collector_selected_fields = [
            "COL_ID",
            "COL_LOGIN",
            "COL_FNAME",
            "COL_LNAME",
        ]

        dim_collector = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_collector",
            dim_collector_final_filters,
            dim_collector_selected_fields,
        )

        return dim_collector

    # ============================== qca_dim_packet ====================================

    def qcaDimPacket(self) -> SparkDataFrame:

        dim_packet_extra_filters = None
        if dim_packet_extra_filters is not None:
            dim_packet_extra_filters.append(self.date_filter)
            dim_packet_final_filters = " AND ".join(dim_packet_extra_filters)
        else:
            dim_packet_final_filters = self.date_filter

        dim_packet_selected_fields = [
            "PACK_ID",
            "PACK_ASSIGN_DATE",
        ]

        dim_packet = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_packet",
            dim_packet_final_filters,
            dim_packet_selected_fields,
        )

        return dim_packet

    # ============================== qca_dim_packet_hier3 ====================================

    def qcaDimPacketHier3(self) -> SparkDataFrame:

        dim_packet_hier3_extra_filters = None
        if dim_packet_hier3_extra_filters is not None:
            dim_packet_hier3_extra_filters.append(self.date_filter)
            dim_packet_hier3_final_filters = " AND ".join(dim_packet_hier3_extra_filters)
        else:
            dim_packet_hier3_final_filters = self.date_filter

        dim_packet_hier3_selected_fields = [
            "PACKH3_ID",
            "PACKH3_DESC",
        ]

        dim_packet_hier3 = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_dim_packet_hier3",
            dim_packet_hier3_final_filters,
            dim_packet_hier3_selected_fields,
        )

        return dim_packet_hier3

    # ============================== params_assignment_qualcoab_exceptions ====================================

    def qcaAssignmentExceptions(self) -> SparkDataFrame:

        assignment_exceptions_extra_filters = None
        if assignment_exceptions_extra_filters is not None:
            assignment_exceptions_extra_filters.append(self.date_filter)
            assignment_exceptions_final_filters = " AND ".join(assignment_exceptions_extra_filters)
        else:
            assignment_exceptions_final_filters = self.date_filter

        assignment_exceptions_selected_fields = [
            "QUE_DESC",
        ]

        assignment_exceptions = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "params_assignment_qualcoab_exceptions",
            assignment_exceptions_final_filters,
            assignment_exceptions_selected_fields,
        )

        return assignment_exceptions
    
    # ================== qca_at_case_core ==================
    
    def qcaAtCaseCore(self) -> SparkDataFrame:

        at_case_core_extra_filters = None
        if at_case_core_extra_filters is not None:
            at_case_core_extra_filters.append(self.date_filter)
            at_case_core_final_filters = " AND ".join(at_case_core_extra_filters)
        else:
            at_case_core_final_filters = self.date_filter

        at_case_core_selected_fields = [
            "CASE_CODE",
            "emp_code_assignment",
        ]

        at_case_core = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_at_case_core",
            at_case_core_final_filters,
            at_case_core_selected_fields,
        )

        return at_case_core
    
    # ================== qca_employees ==================

    def qcaEmployees(self) -> SparkDataFrame:

        employees_extra_filters = None
        if employees_extra_filters is not None:
            employees_extra_filters.append(self.date_filter)
            employees_final_filters = " AND ".join(employees_extra_filters)
        else:
            employees_final_filters = self.date_filter

        employees_selected_fields = [
            "emp_code",
            "TEA_CODE",
            "Team_Name",
            "Dep_desc",
        ]

        employees = load_landed_delta_table(
            self.spark,
            self.catalog_landed,
            "qca_employees",
            employees_final_filters,
            employees_selected_fields,
        )

        return employees
    
    # ============================== portfolio ====================================

    def qcaPortfolio(self) -> SparkDataFrame:

        df = self.spark.read.table(f"{self.catalog}.`primary`.Portfolio")
        df = df.filter(f"BusinessDate = '{self.run_date}'")
        df = df.select("portfolio_id_qca", "PortfolioName", "Prefix")
        df = df.dropDuplicates()
        return df

    # ============================== QC Setup ====================================

    def setupQcTempViews(self) -> None:
        """Register QC source tables as temp views for SQL queries."""
        self.qcCaseMaster().createOrReplaceTempView("case_master")
        self.qcCustomer().withColumnRenamed("channel", "customer_channel").createOrReplaceTempView("customer")
        self.qcEmployees().createOrReplaceTempView("employees")
        self.qcStrategyStep().createOrReplaceTempView("strategy_step")
        self.qcExternalCompanies().createOrReplaceTempView("external_companies")
        self.qcAssignmentQueueSetup().createOrReplaceTempView("assignment_queue_setup")

        # Team (filtered for Mercury 3869)
        self.qcTeam().filter(col("TEA_NAME") == "Mercury CA Team N3869").createOrReplaceTempView("team")

        # Team_L3869_Segments from reference
        self.spark.read.table(f"{self.catalog}.`reference`.team_l_3869_segments").createOrReplaceTempView("Team_L3869_Segments")

        # Portfolio from primary
        self.spark.sql(f"""
            SELECT DISTINCT vendor_code_qualco
            FROM {self.catalog}.`primary`.Portfolio
            WHERE BusinessDate = '{self.run_date}'
        """).createOrReplaceTempView("Portfolio")

        # Case External Assignment (aggregated for PACK_ASSIGN_DATE - DT-259)
        cea = self.qcCaseExternalAssignment()
        cea.groupBy("CASE_CODE", "VEND_CODE").agg(
            {"PACK_ASSIGN_DATE": "max"}
        ).withColumnRenamed("max(PACK_ASSIGN_DATE)", "PACK_ASSIGN_DATE").createOrReplaceTempView("case_external_assignment_agg")

    def resolveQcVariables(self) -> dict:
        """Resolve QC SQL variables (equivalent of on-prem DECLARE statements)."""
        # @3869_ca_dialer_team_mercury = (select TEA_CODE from qc.team where TEA_NAME='Mercury CA Team N3869')
        # @3869_ca_dialer_channel_mercury = (select channel_id from qc.team where TEA_CODE=@3869_ca_dialer_team_mercury)
        team_rows = self.spark.sql("SELECT TEA_CODE, channel_id FROM team").collect()
        if team_rows:
            ca_dialer_team = team_rows[0][0]
            ca_dialer_channel = team_rows[0][1]
        else:
            ca_dialer_team = -1
            ca_dialer_channel = -1

        # @ChannelCollectionAgents - use qc_department as fallback since dwh.ref.Channel not available
        dept_rows = self.qcDepartment().filter(
            col("dep_desc") == "Collection Agents"
        ).select("dep_code").collect()
        channel_collection_agents = dept_rows[0][0] if dept_rows else 105

        return {
            "ca_dialer_team_mercury_3869": ca_dialer_team,
            "ca_dialer_channel_mercury_3869": ca_dialer_channel,
            "channel_collection_agents": channel_collection_agents,
        }

    # ============================== QCA Setup ====================================

    def setupQcaTempViews(self) -> None:
        """Register QCA source tables as temp views for SQL queries."""
        self.qcaFsCasesSnap().createOrReplaceTempView("fs_cases_snap")
        self.qcaDimCasesStatic().createOrReplaceTempView("dim_cases_static")
        self.qcaDimExternalCompany().createOrReplaceTempView("dim_external_company")
        self.qcaDimBranch().createOrReplaceTempView("dim_branch")
        self.qcaDimQueue().createOrReplaceTempView("dim_queue")
        self.qcaDimCollector().createOrReplaceTempView("dim_Collector")
        self.qcaDimPacket().createOrReplaceTempView("dim_packet")
        self.qcaDimPacketHier3().dropDuplicates(["PACKH3_ID", "PACKH3_DESC"]).createOrReplaceTempView("dim_packet_hier3")
        self.qcaAssignmentExceptions().createOrReplaceTempView("assignment_qualcoab_exceptions")
        self.qcaPortfolio().createOrReplaceTempView("portfolio")
        self.qcaEmployees().createOrReplaceTempView("employees")
        self.qcaAtCaseCore().createOrReplaceTempView("at_case_core")

        # assignment_queue_setup_qca from reference
        self.spark.read.table(
            f"{self.catalog}.`reference`.params_assignment_queue_setup_qca"
        ).createOrReplaceTempView("assignment_queue_setup_qca")

    # ============================== Load Curated Assignment QCA ====================================

    def loadCuratedAssignmentQca(self) -> SparkDataFrame:
        """Build the CaseAssignmentQCA DataFrame and return it (without writing to table).
        
        Reads galaxycoreperimeter from landed zone and uses it for the QCA assignment queries.
        """
        # Disable ANSI mode to handle empty string casts gracefully
        self.spark.conf.set("spark.sql.ansi.enabled", "false")

        # Setup QCA temp views
        self.setupQcaTempViews()

        # Galaxy Core Perimeter — read directly from landed zone (imported from on-prem)
        self.spark.sql(f"""
            SELECT account_number, Galaxy_core, IsActiveFlag
            FROM {self.catalog_landed}.adm_galaxycoreperimeter
            WHERE BusinessDate = '{self.run_date}'
        """).createOrReplaceTempView("galaxycoreperimeter")

        # ==========================================================================

        # Whenever the Business requests the implementation of a new rule for a new
        # portfolio, the corresponding logic must be implemented in a dedicated query
        # (e.g., s7) and added below the existing queries.
        #
        # IMPORTANT:
        # When adding a new portfolio-specific query, the new portfolio must also be
        # excluded from query s1. This exclusion must be applied to the following
        # condition:
        #
        #     cases.SPET_CODE NOT IN (18, 20, 22, 28, 26, 25, 29, 30, 27, 24, 31)
        #
        # For example, if new business rules are introduced for the Athena portfolio
        # in query s6, its corresponding portfolio identifier must be excluded from
        # query s1.
        #
        # For Athena:
        #     PortfolioKey = portfolio_id_qca = SPET_CODE = 61
        #
        # Therefore, SPET_CODE = 61 must be added to the exclusion list in query s1:
        #
        #     cases.SPET_CODE NOT IN (18, 20, 22, 28, 26, 25, 29, 30, 27, 24, 31, 61)
        #
        # This ensures that records belonging to the new portfolio are processed
        # exclusively by the dedicated portfolio-specific query and are not also
        # processed by the generic logic defined in query s1.

        # ================== s1: NOT CORE Securitized ===============================

        s1 = self.spark.sql("""
            SELECT * FROM (
            SELECT 
                cast(cases.CASE_CODE as string) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                cast(cases.CUST_CODE as string) AS CUST_CODE,
                CASE
                    WHEN q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN 'Outsourcing Collection'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND q.QUE_DESC ilike '%NET' THEN 'FAs'
                    WHEN q.QUE_DESC ilike 'ΔΤ3869%' AND exc.QUE_DESC IS NULL THEN 'L3869 ΔΤ - FAs'
                    WHEN q.QUE_DESC ilike '%ext'
                        AND ext.EXTC_NAME NOT ilike 'PRE%'
                        AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK'
                        AND ext.EXTC_NAME NOT ilike 'N3869%'
                    THEN 'Legal Agencies'
                    ELSE '-1'
                END AS AssignmentChannel,
                cast(cases.EXTC_CODE_PRIMARY as string) AS ExternalCompanyAssignment,
                ext.EXTC_NAME AS ExternalCompanyName,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN ext.EXTC_NAME
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND q.QUE_DESC ilike '%NET' THEN br.BRN_DESC
                    WHEN q.QUE_DESC ilike 'ΔΤ3869%' AND exc.QUE_DESC IS NULL THEN br.BRN_DESC
                    WHEN q.QUE_DESC ilike '%ext'
                        AND ext.EXTC_NAME NOT ilike 'PRE%'
                        AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK'
                    THEN ext.EXTC_NAME
                    ELSE '-1'
                END AS AssignmentTeam,
                CASE
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' OR q.QUE_DESC ilike '%NET'
                    THEN cast(cases.EMP_TAG as string)
                    ELSE '-1'
                END AS AssignmentAgent,
                col.COL_LOGIN AS AssignmentAgentLoginName,
                br.BRN_INTCODE AS Branch,
                cast(null as string) AS BranchDescription,
                'N/A' AS AssignmentProcess,
                cases.SPET_CODE AS portfolio_id_qca,
                dp.PACK_ASSIGN_DATE
            FROM fs_cases_snap cases
            INNER JOIN dim_packet dp ON dp.PACK_ID = cases.PACK_CODE
            INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
                AND dcases.CASE_CURRENTLY_OPEN = 1
            INNER JOIN portfolio portfolio
                ON CAST(cases.SPET_CODE AS INT) = portfolio.portfolio_id_qca 
                AND portfolio.PortfolioName NOT IN ('Core Retail', 'Core Wholesale')
                AND portfolio.Prefix NOT IN ('ORB', 'LGT', 'SIG', 'SIV', 'CEL', 'PRA')
            LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
            LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
            LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
            LEFT JOIN assignment_qualcoab_exceptions exc ON exc.QUE_DESC = q.QUE_DESC
            LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
            WHERE cases.SPET_CODE NOT IN ( 18, 20, 22, 28, 26, 25, 29, 30, 27, 24, 31, 61)
            ) A
            WHERE cast(A.AssignmentChannel as string) <> '-1'
            OR A.AssignmentAgent IS NOT NULL
        """)

        # ================== s2: CORE Banking ==================
        s2 = self.spark.sql("""
            SELECT * FROM (
            SELECT 
                cast(cases.CASE_CODE as string) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                cast(cases.CUST_CODE as string) AS CUST_CODE,
                CASE
                    WHEN (q.QUE_DESC ilike '%net') AND (ext.EXTC_NAME ilike 'BRANCH_NETWORK' OR q.QUE_DESC ilike 'ΔΤ3869_hub%') THEN 'HUB RMs'
                    WHEN q.QUE_DESC ilike '%net_int' AND ext.EXTC_NAME ilike 'BRANCH_NETWORK' THEN 'HUB RMs / Call Center'
                    WHEN (q.QUE_DESC ilike '%int' AND q.QUE_DESC NOT ilike '%IRT%')
                        OR q.QUE_DESC ilike 'IRT_Dou%'
                        OR LOWER(q.QUE_DESC) = 'sse84_loans'
                        OR q.QUE_DESC = 'Private_Affluent_Customers'
                        OR q.QUE_DESC = 'Settlements_Προσωρινών_Διαταγών_3869'
                        OR q.QUE_DESC = 'Gefyra_Accounts'
                    THEN 'Call Center'
                    WHEN q.QUE_DESC ilike '%int' AND q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN 'Outsourcing Collection'
                    WHEN q.QUE_DESC ilike '%ext'
                        AND ext.EXTC_NAME NOT ilike 'PRE%'
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK'
                        AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME IN ('ANDRIKOPOULOS','GIANNINI','MOURGELAS','SIOUFAS','KORAKI','LEXIS','TSAFARAS')
                    THEN 'Legal Agencies'
                    WHEN ext.EXTC_NAME ilike 'N3869%' THEN 'Legal Agencies 3869'
                    ELSE '-1'
                END AS AssignmentChannel,
                cast(cases.EXTC_CODE_PRIMARY as string) AS ExternalCompanyAssignment,
                ext.EXTC_NAME AS ExternalCompanyName,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN ext.EXTC_NAME
                    WHEN ((q.QUE_DESC ilike '%net') AND (ext.EXTC_NAME ilike 'BRANCH_NETWORK' OR q.QUE_DESC ilike 'ΔΤ3869_hub%'))
                        OR (q.QUE_DESC ilike '%net_int' AND ext.EXTC_NAME ilike 'BRANCH_NETWORK')
                    THEN br.BRN_DESC
                    WHEN q.QUE_DESC ilike '%ext'
                        AND ext.EXTC_NAME NOT ilike 'PRE%'
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK'
                        AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME IN ('ANDRIKOPOULOS','GIANNINI','MOURGELAS','SIOUFAS','KORAKI','LEXIS','TSAFARAS')
                    THEN ext.EXTC_NAME
                    ELSE '-1'
                END AS AssignmentTeam,
                CASE
                    WHEN ((q.QUE_DESC ilike '%net') AND (ext.EXTC_NAME ilike 'BRANCH_NETWORK' OR q.QUE_DESC ilike 'ΔΤ3869_hub%'))
                        OR (q.QUE_DESC ilike '%net_int' AND ext.EXTC_NAME ilike 'BRANCH_NETWORK')
                    THEN cast(cases.EMP_TAG as string)
                    ELSE '-1'
                END AS AssignmentAgent,
                col.COL_LOGIN AS AssignmentAgentLoginName,
                br.BRN_INTCODE AS Branch,
                cast(null as string) AS BranchDescription,
                'N/A' AS AssignmentProcess,
                cases.SPET_CODE AS portfolio_id_qca,
                cast(NULL as string) AS PACK_ASSIGN_DATE
            FROM fs_cases_snap cases
            INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
                AND dcases.CASE_CURRENTLY_OPEN = 1
            INNER JOIN portfolio portfolio
                ON CAST(cases.SPET_CODE AS INT) = portfolio.portfolio_id_qca
                AND portfolio.PortfolioName IN ('Core Retail', 'Core Wholesale')
                AND (portfolio.Prefix NOT IN ('ORB', 'LGT', 'SIG', 'SIV', 'CEL', 'PRA') OR portfolio.Prefix IS NULL)     
            LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
            LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
            LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
            LEFT JOIN assignment_qualcoab_exceptions exc ON exc.QUE_DESC = q.QUE_DESC
            LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
            ) A
            WHERE cast(A.AssignmentChannel as string) <> '-1'
        """)

        # ================== s3: Domus/Rhodium Retail (SPET_CODE IN 20,22) ==================
        s3 = self.spark.sql("""
            SELECT 
                cast(cases.CASE_CODE as string) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                cast(cases.CUST_CODE as string) AS CUST_CODE,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN 'Outsourcing Collection'
                    WHEN ext.EXTC_NAME NOT ilike 'PRE%' AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK' AND ext.EXTC_NAME NOT ilike 'N3869%'
                    THEN 'Legal Agencies'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%FAs%' AND br.BRN_DESC NOT ilike '%High%' THEN 'FAs'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%High%' THEN 'FAs High Balance'
                    WHEN q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    ELSE NULL
                END AS AssignmentChannel,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN NULL ELSE cast(cases.EXTC_CODE_PRIMARY as string) END AS ExternalCompanyAssignment,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN NULL ELSE ext.EXTC_NAME END AS ExternalCompanyName,
                CASE WHEN ext.EXTC_NAME <> 'BRANCH_NETWORK' THEN NULL ELSE br.BRN_DESC END AS AssignmentTeam,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN cast(cases.EMP_TAG as string) ELSE NULL END AS AssignmentAgent,
                col.COL_LOGIN AS AssignmentAgentLoginName,
                br.BRN_INTCODE AS Branch,
                cast(null as string) AS BranchDescription,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN 'Collection'
                    WHEN ext.EXTC_NAME NOT ilike 'PRE%' AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK' AND ext.EXTC_NAME NOT ilike 'N3869%' THEN 'LF_Offer'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%FAs%' AND br.BRN_DESC NOT ilike '%High%' THEN 'CA/FA/Sme Officer'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%High%' THEN 'CA/FA/Sme Officer'
                    WHEN q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    WHEN exc.assignment_mapping IS NOT NULL THEN exc.assignment_mapping
                    ELSE 'Rest'
                END AS AssignmentProcess,
                cases.SPET_CODE AS portfolio_id_qca,
                cast(NULL as string) AS PACK_ASSIGN_DATE
            FROM fs_cases_snap cases
            INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
                AND dcases.CASE_CODE = dcases.CASE_CODE_PRIMARY
            LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
            LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
            LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
            LEFT JOIN assignment_queue_setup_qca exc ON exc.queue_desc = q.QUE_DESC
            LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
            WHERE cases.SPET_CODE IN (20, 22)
        """)

        # ================== s4: Monza Retail (SPET_CODE = 18) ==================
        s4 = self.spark.sql("""
            SELECT 
                cast(cases.CASE_CODE as string) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                cast(cases.CUST_CODE as string) AS CUST_CODE,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' THEN 'Outsourcing Collection'
                    WHEN ext.EXTC_NAME NOT ilike 'PRE%' AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK' AND ext.EXTC_NAME NOT ilike 'N3869%'
                    THEN 'Legal Agencies'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%FAs%' AND br.BRN_DESC NOT ilike '%High%' THEN 'FAs'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%High%' THEN 'FAs High Balance'
                    WHEN q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    ELSE NULL
                END AS AssignmentChannel,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN NULL ELSE cast(cases.EXTC_CODE_PRIMARY as string) END AS ExternalCompanyAssignment,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN NULL ELSE ext.EXTC_NAME END AS ExternalCompanyName,
                CASE WHEN ext.EXTC_NAME <> 'BRANCH_NETWORK' THEN NULL ELSE br.BRN_DESC END AS AssignmentTeam,
                CASE WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' THEN cast(cases.EMP_TAG as string) ELSE NULL END AS AssignmentAgent,
                col.COL_LOGIN AS AssignmentAgentLoginName,
                br.BRN_INTCODE AS Branch,
                cast(null as string) AS BranchDescription,
                CASE
                    WHEN ext.EXTC_NAME ilike 'PRE%' AND packet_hier3.PACKH3_DESC = 'Monza_Collection' THEN 'Collection'
                    WHEN ext.EXTC_NAME NOT ilike 'PRE%' AND ext.EXTC_NAME IS NOT NULL
                        AND ext.EXTC_NAME <> 'BRANCH_NETWORK' AND ext.EXTC_NAME NOT ilike 'N3869%'
                        AND packet_hier3.PACKH3_DESC = 'Monza_Restructuring_LFs' THEN 'LF_Offer'
                    WHEN ext.EXTC_NAME ilike 'PRE%' AND packet_hier3.PACKH3_DESC = 'Monza_Restructuring_DNCs' THEN 'DNC_Offer'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%FAs%' AND br.BRN_DESC NOT ilike '%High%'
                        AND packet_hier3.PACKH3_DESC = 'Monza_Restructuring_FAs' THEN 'CA/FA/Sme Officer'
                    WHEN ext.EXTC_NAME = 'BRANCH_NETWORK' AND cases.EMP_TAG IS NOT NULL
                        AND br.BRN_DESC ilike '%High%'
                        AND packet_hier3.PACKH3_DESC = 'Monza_Restructuring_FAs' THEN 'CA/FA/Sme Officer'
                    WHEN q.QUE_DESC ilike '%IRT%' THEN 'IRT'
                    WHEN exc.assignment_mapping IS NOT NULL THEN exc.assignment_mapping
                    ELSE 'Rest'
                END AS AssignmentProcess,
                cases.SPET_CODE AS portfolio_id_qca,
                cast(NULL as string) AS PACK_ASSIGN_DATE
            FROM fs_cases_snap cases
            INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
                AND dcases.CASE_CODE = dcases.CASE_CODE_PRIMARY
            INNER JOIN dim_packet packet ON cases.PACK_CODE = packet.PACK_ID
            INNER JOIN dim_packet_hier3 packet_hier3 ON cases.PACKH3_ID = packet_hier3.PACKH3_ID
            LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
            LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
            LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
            LEFT JOIN assignment_queue_setup_qca exc ON exc.queue_desc = q.QUE_DESC
            LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
            WHERE cases.SPET_CODE = 18
        """)

        # ================== Union ==================
        # ================== s5: Hoist Portfolios (SPET_CODE IN 28,26,25,29,30,27,24,31) ==================
        s5 = self.spark.sql("""
            SELECT 
                cast(cases.CASE_CODE as string) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                cast(cases.CUST_CODE as string) AS CUST_CODE,
                CASE
                    WHEN packet_hier3.PACKH3_DESC ilike '%offer%' AND ext.EXTC_NAME IN ('CHARAKTINIOTIS','MARAKAKIS','SIOUFAS','PSYCHAS','STAVROPOULOU') THEN 'Legal Agencies'
                    WHEN packet_hier3.PACKH3_DESC ilike '%offer%' AND ext.EXTC_NAME IN ('PRE_TELEPERFORMANCE','PRE_PALADINO') THEN 'Outsourcing Collection'
                    WHEN packet_hier3.PACKH3_DESC ilike 'Collection%' OR packet_hier3.PACKH3_DESC IN ('L3869_Exp_Balloon_Hoist_only','L3869_PAYERS_ANDRIKOPOULOS','L3869_Exp_Balloon_Hoist_mixed') THEN 'Outsourcing Collection'
                    WHEN cases.EMP_TAG IS NOT NULL AND employees.Team_Name ILIKE '% CA %' THEN 'Collection Agents'
                    WHEN cases.EMP_TAG IS NOT NULL AND employees.Team_Name ILIKE '%IRT%' THEN 'IRT'
                    WHEN cases.EMP_TAG IS NOT NULL THEN 'Financial Advisors'
                    WHEN q.QUE_DESC ilike '%CA_int' THEN 'Ca Dialer'
                    ELSE employees.Dep_desc
                END AS AssignmentChannel,
                cast(cases.EXTC_CODE_PRIMARY as string) AS ExternalCompanyAssignment,
                ext.EXTC_NAME AS ExternalCompanyName,
                CASE
                    WHEN packet_hier3.PACKH3_DESC ilike '%offer%' AND ext.EXTC_NAME IN ('CHARAKTINIOTIS','MARAKAKIS','SIOUFAS','PSYCHAS','STAVROPOULOU','PRE_TELEPERFORMANCE','PRE_PALADINO') THEN ext.EXTC_NAME
                    WHEN packet_hier3.PACKH3_DESC ilike 'Collection%' OR packet_hier3.PACKH3_DESC IN ('L3869_Exp_Balloon_Hoist_only','L3869_PAYERS_ANDRIKOPOULOS','L3869_Exp_Balloon_Hoist_mixed') THEN ext.EXTC_NAME
                    ELSE employees.Team_Name
                END AS AssignmentTeam,
                CASE WHEN cases.EMP_TAG IS NOT NULL THEN cast(cases.EMP_TAG as string) ELSE '-1' END AS AssignmentAgent,
                col.COL_LOGIN AS AssignmentAgentLoginName,
                br.BRN_INTCODE AS Branch,
                cast(null as string) AS BranchDescription,
                CASE
                    WHEN packet_hier3.PACKH3_DESC ilike '%offer%' AND ext.EXTC_NAME IN ('CHARAKTINIOTIS','MARAKAKIS','SIOUFAS','PSYCHAS','STAVROPOULOU') THEN 'LF_Offer'
                    WHEN packet_hier3.PACKH3_DESC ilike '%offer%' AND ext.EXTC_NAME IN ('PRE_TELEPERFORMANCE','PRE_PALADINO') THEN 'DNC_Offer'
                    WHEN packet_hier3.PACKH3_DESC ilike 'Collection%' OR packet_hier3.PACKH3_DESC IN ('L3869_Exp_Balloon_Hoist_only','L3869_PAYERS_ANDRIKOPOULOS','L3869_Exp_Balloon_Hoist_mixed') THEN 'Collection'
                    WHEN cases.EMP_TAG IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN q.QUE_DESC ilike '%CA_int' THEN 'CA Dialer'
                    WHEN q.QUE_DESC ilike '%Paid_Off%' OR q.QUE_DESC ilike '%Fulfilled%' THEN 'Fulfilled Settlment / Paid Off'
                    WHEN q.QUE_DESC ilike '%Vulnerable%' THEN 'Passive Vulnerable'
                    WHEN q.QUE_DESC ilike '%Bankruptcy%' THEN 'Passive Bankruptcy'
                    WHEN q.QUE_DESC ilike '% Bucket_00%' OR q.QUE_DESC ilike '%_Non-Delinquent%' THEN 'Passive_Current B0'
                    WHEN q.QUE_DESC ilike '% default%' THEN 'Passive_system exclusion'
                    WHEN q.QUE_DESC ilike '% Grace%' THEN 'Passive_low debt amt'
                    WHEN q.QUE_DESC ilike '%Accepted_SR%' THEN 'Passive 3869'
                    WHEN q.QUE_DESC ilike '%Balance <50%' THEN 'Passive_low Balance'
                    WHEN q.QUE_DESC ilike '%OCW%' THEN 'Passive OCW'
                    WHEN q.QUE_DESC ilike '%state_guaranteed%' THEN 'Passive_State Guaranteed'
                    WHEN q.QUE_DESC ilike '%4605%' THEN 'Passive 4605'
                    WHEN q.QUE_DESC ilike '%WDN%' THEN 'Passive_slaves Accounts'
                    WHEN q.QUE_DESC ilike '%Under_11_dpds%' THEN 'Exclusion 10dpd'
                    WHEN q.QUE_DESC IN ('L3869 Appeals No Final Court Decision','L3869 Court Discussion Reschedule Application','L3869 No Court Decision Application','N.3869_Έναρξη_Διαδικασίας_Έκπτωσης','N.3869_Εξοφλήσεις-Ακυρώσεις','N3869_ForDefault','N3869_Έναρξη_Διαδικασίας_Έκπτωσης') OR q.QUE_DESC ilike 'L3869 Pos Commen Day_%' OR q.QUE_DESC ilike 'L3869 Pos No Payment_%' OR q.QUE_DESC ilike 'L3869 Recalled cases do not call_%' THEN 'Passive 3869'
                    WHEN q.QUE_DESC = 'Cancelled_Cards_With_Only_Subscription' THEN 'Exlclusion_Cards_With_Only_Subscription'
                    WHEN q.QUE_DESC ilike '%Slaves%' OR q.QUE_DESC ilike '%Λογαριασμοί_Υπό_Αίρεση%' THEN 'Exclusion_slaves'
                    WHEN q.QUE_DESC = 'Disputes' THEN 'Exclusion Dispute'
                    WHEN q.QUE_DESC = 'Uncofimed Deceased' THEN 'Exclusion_Uncofimed Deceased'
                    WHEN q.QUE_DESC ilike '%Συμβιβαστικές Εξοφλήσεις%' THEN 'Exclusion_claimed to have paid off'
                    WHEN q.QUE_DESC = 'Erroneously activated Standing Orders' THEN 'Exlcusion_Complaint'
                    WHEN q.QUE_DESC ilike '%Gefyra%' THEN 'Exlcusion_Gefyra account'
                    WHEN q.QUE_DESC ilike '%Παράπονα%' OR q.QUE_DESC = 'Ισχυρισμός Εξόφλησης' THEN 'Exclusion Complaint'
                    WHEN q.QUE_DESC IN ('Never Added to Pledge Registry','Placed Back Cases to Seller') THEN 'Exclusion_no active perimeter'
                    WHEN q.QUE_DESC = 'Special Handling' THEN 'Exclusion Special Handling'
                    WHEN q.QUE_DESC = 'Special_Treatment' THEN 'Exclusion Special Treatment'
                    WHEN q.QUE_DESC = 'Temp_Exception_from_Assignment' THEN 'Exclusions Queues List'
                    WHEN q.QUE_DESC ilike 'Αναστολή Όχλησης%' OR q.QUE_DESC = 'Αναστολή όχλησης by Asset_Owner' THEN 'Exlusion_not eligible to be contacted'
                    WHEN q.QUE_DESC = 'Αναστολή Όχλησης_Πληγέντων' THEN 'Exlusion_Physical disaster'
                    WHEN q.QUE_DESC = 'Αναστολή_Όχλησης_AML' THEN 'Exlusion_AML'
                    ELSE 'Unhandled'
                END AS AssignmentProcess,
                cases.SPET_CODE AS portfolio_id_qca,
                dp.PACK_ASSIGN_DATE
            FROM fs_cases_snap cases
            INNER JOIN dim_packet dp ON dp.PACK_ID = cases.PACK_CODE
            INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
                AND dcases.CASE_CURRENTLY_OPEN = 1
            INNER JOIN portfolio portfolio
                ON CAST(cases.SPET_CODE AS INT) = portfolio.portfolio_id_qca
            LEFT JOIN at_case_core core ON core.CASE_CODE = cases.CASE_CODE
            LEFT JOIN employees ON employees.emp_code = core.emp_code_assignment
            LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
            LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
            LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
            LEFT JOIN dim_packet_hier3 packet_hier3 ON packet_hier3.PACKH3_ID = cases.PACKH3_ID
            LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
            WHERE cases.SPET_CODE IN (28, 26, 25, 29, 30, 27, 24, 31)
        """)

        # [ADDED]: 01/10/2026
        # [AUTHOR]: V. Iurcovschi
        # [NOTES]:
        # The new business rules for the columns AssignmentProcess, ExternalCompanyName, and AssignmentAgent have been provided by Business (Check TXN-339 ticket).
        # The requested implementation is based on the existing implementation currently used for HOIST portfolios.
        # The mapping between the columns returned by the internal layer and those returned by the Assignment QCA notebook is provided below:
        # - AssignmentProcess: AssignmentProcess
        # - ExternalCompanyName: ExternalCompanyFullName
        # - AssignmentAgent: AgentFullName 
        
        # [TO DO]:
        # The implementation follows the existing coding style for the tables s1 through s6. 
        # However, it has been agreed that each internal layer must contain PySpark code exclusively and must not include SQL code.
        # The current implementation uses SQL and therefore does not comply with the agreed coding standards. 
        # This section should be refactored and rewritten using PySpark.
        
        # =============================== s6: Athena Portfolio (SPET_CODE = 61) ==================================

        s6 = self.spark.sql("""
        WITH withAssignmentRawData AS (
        SELECT CAST(cases.CASE_CODE AS STRING) AS CASE_CODE,
                dcases.CASE_ACCOUNT_FOR_SEARCH,
                CAST(cases.CUST_CODE AS STRING) AS CUST_CODE,
                cases.EMP_TAG,
                CAST(cases.EXTC_CODE_PRIMARY AS STRING) AS EXTC_CODE_PRIMARY,
                cases.SPET_CODE,
                packet_hier3.PACKH3_DESC,
                ext.EXTC_NAME,
                employees.Team_Name,
                employees.Dep_desc,
                q.QUE_DESC,
                col.COL_LOGIN,
                br.BRN_INTCODE,
                dp.PACK_ASSIGN_DATE,
                COALESCE(
                    NULLIF(CONCAT_WS(' ', TRIM(col.COL_FNAME), TRIM(col.COL_LNAME)), ''),
                    NULLIF(TRIM(col.COL_FNAME), ''),
                    NULLIF(TRIM(col.COL_LNAME), '')
                ) AS Agent_Full_Name
        FROM fs_cases_snap cases
        INNER JOIN dim_packet dp ON dp.PACK_ID = cases.PACK_CODE
        INNER JOIN dim_cases_static dcases ON dcases.CASE_CODE = cases.CASE_CODE
        AND dcases.CASE_CURRENTLY_OPEN = 1
        INNER JOIN portfolio portfolio
        ON CAST(cases.SPET_CODE AS INT) = portfolio.portfolio_id_qca
        LEFT JOIN at_case_core core ON core.CASE_CODE = cases.CASE_CODE
        LEFT JOIN employees ON employees.emp_code = core.emp_code_assignment
        LEFT JOIN dim_external_company ext ON ext.EXTC_ID = cases.EXTC_CODE_PRIMARY
        LEFT JOIN dim_branch br ON cases.CASE_TAG = br.BRN_ID
        LEFT JOIN dim_queue q ON q.QUE_ID = cases.QUE_CODE
        LEFT JOIN dim_packet_hier3 packet_hier3 ON packet_hier3.PACKH3_ID = cases.PACKH3_ID
        LEFT JOIN dim_Collector col ON col.COL_ID = cases.EMP_TAG
        WHERE cases.SPET_CODE  = 61
        ),

        withAssignmentProcessRule AS (
        SELECT CASE_CODE,
            CASE_ACCOUNT_FOR_SEARCH,
            CUST_CODE,
            CASE
                WHEN PACKH3_DESC ilike '%offer%' AND EXTC_NAME IN ('CHARAKTINIOTIS','MARAKAKIS','SIOUFAS','PSYCHAS','STAVROPOULOU') THEN 'Legal Agencies'
                WHEN PACKH3_DESC ilike '%offer%' AND EXTC_NAME IN ('PRE_TELEPERFORMANCE','PRE_PALADINO') THEN 'Outsourcing Collection'
                WHEN PACKH3_DESC ilike 'Collection%' OR PACKH3_DESC IN ('L3869_Exp_Balloon_Hoist_only','L3869_PAYERS_ANDRIKOPOULOS','L3869_Exp_Balloon_Hoist_mixed') THEN 'Outsourcing Collection'
                WHEN EMP_TAG IS NOT NULL AND Team_Name ILIKE '% CA %' THEN 'Collection Agents'
                WHEN EMP_TAG IS NOT NULL AND Team_Name ILIKE '%IRT%' THEN 'IRT'
                WHEN EMP_TAG IS NOT NULL THEN 'Financial Advisors'
                WHEN QUE_DESC ilike '%CA_int' THEN 'Ca Dialer'
                ELSE Dep_desc END AS AssignmentChannel,
            EXTC_CODE_PRIMARY AS ExternalCompanyAssignment,
            EXTC_NAME,
            CASE 
                WHEN PACKH3_DESC ilike '%offer%' AND EXTC_NAME IN ('CHARAKTINIOTIS','MARAKAKIS','SIOUFAS','PSYCHAS','STAVROPOULOU','PRE_TELEPERFORMANCE','PRE_PALADINO') THEN EXTC_NAME
                WHEN PACKH3_DESC ilike 'Collection%' OR PACKH3_DESC IN ('L3869_Exp_Balloon_Hoist_only','L3869_PAYERS_ANDRIKOPOULOS','L3869_Exp_Balloon_Hoist_mixed') THEN EXTC_NAME
                ELSE Team_Name END AS AssignmentTeam,
            COL_LOGIN AS AssignmentAgentLoginName,
            BRN_INTCODE AS Branch,
            cast(null as string) AS BranchDescription,
            CASE WHEN PACKH3_DESC ILIKE '%Collection%' THEN 'collection' 
                WHEN Agent_Full_Name IS NOT NULL AND Agent_Full_Name NOT LIKE 'N/A' THEN 'CA/FA/Sme Officer'
                WHEN QUE_DESC ILIKE '%.ext%' AND EXTC_NAME NOT IN ('PRE_PALADINO','PRE_TELEPERFORMANCE','PRE_MELLON','PRE_FIRST_CALL') THEN 'LF_Offer'
                WHEN QUE_DESC ILIKE '%.ext%' AND EXTC_NAME IN ('PRE_PALADINO','PRE_TELEPERFORMANCE','PRE_MELLON','PRE_FIRST_CALL') THEN 'DNC_Offer'
                WHEN QUE_DESC ILIKE '%.int%' THEN 'CA Dialer'
                ELSE 'Passive' END AS AssignmentProcess,
            SPET_CODE AS portfolio_id_qca,
            PACK_ASSIGN_DATE,
            Agent_Full_Name
        FROM withAssignmentRawData
        )

        SELECT CASE_CODE,
                CASE_ACCOUNT_FOR_SEARCH,
                CUST_CODE,
                AssignmentChannel,
                ExternalCompanyAssignment,
                CASE WHEN AssignmentProcess IN ('collection', 'LF_Offer', 'DNC_Offer') THEN EXTC_NAME
                    WHEN AssignmentProcess = 'CA Dialer' THEN 'Dialer'
                    WHEN AssignmentProcess = 'CA/FA/Sme Officer' THEN NULL
                    WHEN AssignmentProcess = 'Passive' THEN 'N/a' END AS ExternalCompanyName,
                AssignmentTeam,
                CASE WHEN AssignmentProcess = 'CA/FA/Sme Officer' THEN Agent_Full_Name
                     WHEN AssignmentProcess = 'Passive' THEN 'N/a'
                     ELSE NULL END AS AssignmentAgent,
                AssignmentAgentLoginName,
                Branch,
                BranchDescription,
                AssignmentProcess,
                portfolio_id_qca,
                PACK_ASSIGN_DATE
        FROM withAssignmentProcessRule
        """)

        # ================== UNION ==================
        combined_df = (
            s1.unionByName(s2, allowMissingColumns=True)
            .unionByName(s3, allowMissingColumns=True)
            .unionByName(s4, allowMissingColumns=True)
            .unionByName(s5, allowMissingColumns=True)
            .unionByName(s6, allowMissingColumns=True)
            .dropDuplicates()
        )

        combined_df = combined_df.withColumn("BusinessDate", lit(self.run_date).cast("date"))
        combined_df = combined_df.withColumn("LoadingDateTime", from_utc_timestamp(current_timestamp(), "Europe/Athens"))

        # Select columns in final order
        combined_df = combined_df.select(
            "CASE_CODE",
            "CASE_ACCOUNT_FOR_SEARCH",
            "CUST_CODE",
            "AssignmentChannel",
            "ExternalCompanyAssignment",
            "ExternalCompanyName",
            "AssignmentTeam",
            "AssignmentAgent",
            "AssignmentAgentLoginName",
            "Branch",
            "BranchDescription",
            "AssignmentProcess",
            "portfolio_id_qca",
            "PACK_ASSIGN_DATE",
            "BusinessDate",
            "LoadingDateTime",
        )

        return combined_df

    # ============================== Load Curated Assignment QC ====================================

    def loadCuratedAssignmentQc(self) -> SparkDataFrame:
        """Build the AssignmentQC DataFrame and return it (without writing to table).
        
        Calls setupQcTempViews() and resolveQcVariables() internally.
        """

        # Setup temp views
        self.setupQcTempViews()

        # Resolve variables
        variables = self.resolveQcVariables()
        mercury_ca_dialer_team = variables.get("ca_dialer_team_mercury_3869", -1)
        mercury_ca_dialer_channel = variables.get("ca_dialer_channel_mercury_3869", -1)
        channel_collection_agents = variables.get("channel_collection_agents", 105)

        # ================== MERCURY (vend_code IN 6,7,8,9) ==================
        mercury = self.spark.sql(f"""
            SELECT DISTINCT
                cm.vend_code, cm.debt_code, cm.case_code, cm.que_desc,
                cu.Segment, cm.Extra_Segment, cm.process_type, cm.emp_code_assignment,
                CASE WHEN ec.extc_code IN (76,81,83,87,88,104,116) THEN ec.extc_code ELSE cm.extc_code END AS extc_code,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='MRC_Offer' AND cm.vend_code=6 THEN 'DNC_Offer'
                    WHEN (cm.process_type IN ('MRC_N3869','MRC_Offer') AND ec.extc_code IN (76,81,83,87,88,104,116))
                        OR (COALESCE(cm.process_type,'')='' AND ec.extc_code IN (76,81,83,87,88,104,116) AND cm.emp_code_assignment IS NULL AND cu.Segment NOT ILIKE '%CP') THEN 'LF_Offer'
                    WHEN cm.vend_code=6 AND cm.extc_code IN (81,87,105,106) AND cm.process_type ILIKE '%collection%' THEN 'Collection'
                    WHEN cm.vend_code=6 AND cm.que_desc='MRC_Coll_Jud_Escalation' AND cm.extc_code IN (81,87) THEN 'Collection_Judicial_Escalation'
                    WHEN cm.emp_code_assignment IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN aqs.assignment_mapping<>'CA Dialer' AND aqs.assignment_mapping IS NOT NULL THEN aqs.assignment_mapping
                    WHEN l3869.team IS NOT NULL AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN
                        CASE WHEN l3869.segment='L3869 Court Approved' THEN
                            CASE WHEN cm.Sub_Segment IN ('Leverage A','Leverage B') THEN 'Passive_Court Approved' ELSE '3869 CA Dialer' END
                        ELSE l3869.team END
                    WHEN aqs.assignment_mapping='CA Dialer' AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') AND cm.vend_code=6 THEN aqs.assignment_mapping
                    WHEN cu.Segment ILIKE '%CP' AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN 'CA Dialer'
                    WHEN cu.customer_channel='External Law Firms 7' THEN 'Unhandled'
                    WHEN cm.process_type IN ('MRC_Judicial','Mercury 3869 Judicial') AND cu.customer_channel<>'' THEN 'Unhandled'
                    ELSE 'Unhandled'
                END AS assignment_process,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='MRC_Offer' AND cm.vend_code=6 THEN -1
                    WHEN (cm.process_type IN ('MRC_N3869','MRC_Offer') AND ec.extc_code IN (76,81,83,87,88,104,116))
                        OR (COALESCE(cm.process_type,'')='' AND ec.extc_code IN (76,81,83,87,88,104,116) AND cm.emp_code_assignment IS NULL AND cu.Segment NOT ILIKE '%CP') THEN -1
                    WHEN cm.vend_code=6 AND cm.extc_code IN (81,87,105,106) AND cm.process_type ILIKE '%collection%' THEN -1
                    WHEN cm.vend_code=6 AND cm.que_desc='MRC_Coll_Jud_Escalation' AND cm.extc_code IN (81,87) THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.emp_code
                    ELSE -1
                END AS agent_id,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='MRC_Offer' AND cm.vend_code=6 THEN -1
                    WHEN (cm.process_type IN ('MRC_N3869','MRC_Offer') AND ec.extc_code IN (76,81,83,87,88,104,116))
                        OR (COALESCE(cm.process_type,'')='' AND ec.extc_code IN (76,81,83,87,88,104,116) AND cm.emp_code_assignment IS NULL AND cu.Segment NOT ILIKE '%CP') THEN -1
                    WHEN cm.vend_code=6 AND cm.extc_code IN (81,87,105,106) AND cm.process_type ILIKE '%collection%' THEN -1
                    WHEN cm.vend_code=6 AND cm.que_desc='MRC_Coll_Jud_Escalation' AND cm.extc_code IN (81,87) THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.team_id
                    WHEN aqs.assignment_mapping<>'CA Dialer' AND aqs.assignment_mapping IS NOT NULL THEN -1
                    WHEN l3869.team IS NOT NULL AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN {mercury_ca_dialer_team}
                    ELSE -1
                END AS team_id,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='MRC_Offer' AND cm.vend_code=6 THEN ec.extc_code
                    WHEN (cm.process_type IN ('MRC_N3869','MRC_Offer') AND ec.extc_code IN (76,81,83,87,88,104,116))
                        OR (COALESCE(cm.process_type,'')='' AND ec.extc_code IN (76,81,83,87,88,104,116) AND cm.emp_code_assignment IS NULL AND cu.Segment NOT ILIKE '%CP') THEN ec.extc_code
                    WHEN cm.vend_code=6 AND cm.extc_code IN (81,87,105,106) AND cm.process_type ILIKE '%collection%' THEN cm.extc_code
                    WHEN cm.vend_code=6 AND cm.que_desc='MRC_Coll_Jud_Escalation' AND cm.extc_code IN (81,87) THEN cm.extc_code
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.dep_code
                    WHEN aqs.assignment_mapping<>'CA Dialer' AND aqs.assignment_mapping IS NOT NULL THEN -1
                    WHEN l3869.team IS NOT NULL AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN {mercury_ca_dialer_channel}
                    ELSE -1
                END AS channel_id,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='MRC_Offer' AND cm.vend_code=6 THEN -1
                    WHEN (cm.process_type IN ('MRC_N3869','MRC_Offer') AND cm.extc_code IN (76,81,83,87,88,104,116))
                        OR (COALESCE(cm.process_type,'')='' AND cm.extc_code IN (76,81,83,87,88,104,116) AND cm.emp_code_assignment IS NULL AND cu.Segment NOT ILIKE '%CP') THEN -1
                    WHEN cm.vend_code=6 AND cm.extc_code IN (81,87,105,106) AND cm.process_type ILIKE '%collection%' THEN -1
                    WHEN cm.vend_code=6 AND cm.que_desc='MRC_Coll_Jud_Escalation' AND cm.extc_code IN (81,87) THEN cm.extc_code
                    WHEN cm.emp_code_assignment IS NOT NULL THEN COALESCE(cm.extc_code,-1)
                    WHEN aqs.assignment_mapping<>'CA Dialer' AND aqs.assignment_mapping IS NOT NULL THEN -1
                    WHEN l3869.team IS NOT NULL AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN COALESCE(cm.extc_code,-1)
                    WHEN aqs.assignment_mapping='CA Dialer' AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') AND cm.vend_code=6 THEN COALESCE(cm.extc_code,-1)
                    WHEN cu.Segment ILIKE '%CP' AND cm.vend_code=6 AND COALESCE(cm.process_type,'') IN ('','MRC_Judicial','Mercury 3869 Judicial') THEN COALESCE(cm.extc_code,-1)
                    WHEN cm.extc_code=82 THEN COALESCE(cm.extc_code,-1)
                    WHEN cm.process_type IN ('MRC_Judicial','Mercury 3869 Judicial') AND cu.customer_channel<>'' THEN COALESCE(cm.extc_code,-1)
                    ELSE -1
                END AS legal_channel_id,
                cu.customer_channel AS customer_channel,
                CAST(NULL AS SMALLINT) AS final_bucket,
                cea.PACK_ASSIGN_DATE
            FROM case_master cm
            INNER JOIN customer cu ON cu.cust_code = cm.cust_code
            LEFT JOIN employees emp ON emp.emp_code = cm.emp_code_assignment
            LEFT JOIN assignment_queue_setup aqs ON aqs.queue_desc = cm.que_desc
            LEFT JOIN external_companies ec ON LOWER(cu.customer_channel) = LOWER(ec.EXTC_COMMENT)
            LEFT JOIN case_external_assignment_agg cea ON cea.CASE_CODE = cm.case_code AND cea.VEND_CODE = cm.vend_code
            LEFT JOIN team_l3869_segments l3869 ON (cu.Segment ILIKE CONCAT(l3869.segment,'%')
                AND (l3869.extra_segment = cm.Extra_Segment OR l3869.extra_segment = 'Any Value')
                AND cu.Segment <> 'L3869 Rest for service call_CP')
            WHERE cm.vend_code IN (6,7,8,9) AND cm.case_relation_type_code = 2090 AND cm.case_currently_open_flag = true
        """)

        # ================== ORBIT (vend_code=13) ==================
        orbit = self.spark.sql(f"""
            SELECT DISTINCT
                cm.vend_code, cm.debt_code, cm.case_code, cm.que_desc,
                cu.Segment, cm.Extra_Segment, cm.process_type, cm.emp_code_assignment,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='Offer' THEN ec.extc_code
                    WHEN cu.customer_channel IN ('External Law Firms 1','External Law Firms 5','External Law Firms 12','External Law Firms 13') AND cm.process_type='Offer' THEN ec.extc_code
                    ELSE cm.extc_code
                END AS extc_code,
                CASE
                    WHEN cu.customer_channel IN ('External Law Firms 27','External Law Firms 28') AND cm.process_type='Offer' THEN 'DNC_Offer'
                    WHEN cu.customer_channel IN ('External Law Firms 1','External Law Firms 5','External Law Firms 12','External Law Firms 13','External Law Firms 26','External Law Firms 30') AND cm.process_type='Offer' THEN 'LF_Offer'
                    WHEN cm.process_type ILIKE '%collection%' AND cm.extc_code IS NOT NULL THEN 'Collection'
                    WHEN cm.emp_code_assignment IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L3869%' OR cu.Segment ILIKE 'L4605%' THEN 'Passive_Segment'
                    WHEN cu.Segment = '' THEN 'Passive_No_Segment'
                    WHEN aqs.assignment_mapping IS NOT NULL THEN aqs.assignment_mapping
                    WHEN cm.que_desc IN ('Orbit Fulfilled Settlements','Orbit Fulfilled Settlements to Recall') THEN 'Fulfilled Settlment / Paid Off'
                    WHEN cu.customer_channel NOT ILIKE '%Financial Advisors%' AND COALESCE(ec.extc_code,0) NOT IN (105,106) AND cu.customer_channel<>'' THEN 'CA Dialer'
                    ELSE 'Unhandled'
                END AS assignment_process,
                CASE
                    WHEN ec.extc_code IN (105,106) AND cm.process_type='Offer' THEN -1
                    WHEN ec.extc_code IN (76,81,87,88,104,116) AND cm.process_type='Offer' THEN -1
                    WHEN cm.process_type ILIKE '%collection%' AND cm.extc_code IS NOT NULL THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.emp_code
                    ELSE -1
                END AS agent_id,
                CASE
                    WHEN ec.extc_code IN (105,106) AND cm.process_type='Offer' THEN -1
                    WHEN ec.extc_code IN (76,81,87,88,104,116) AND cm.process_type='Offer' THEN -1
                    WHEN cm.process_type ILIKE '%collection%' AND cm.extc_code IS NOT NULL THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.team_id
                    ELSE -1
                END AS team_id,
                CASE
                    WHEN ec.extc_code IN (105,106) AND cm.process_type='Offer' THEN ec.extc_code
                    WHEN ec.extc_code IN (76,81,87,88,104,116) AND cm.process_type='Offer' THEN ec.extc_code
                    WHEN cm.process_type ILIKE '%collection%' AND cm.extc_code IS NOT NULL THEN cm.extc_code
                    WHEN cm.emp_code_assignment IS NOT NULL THEN emp.dep_code
                    ELSE -1
                END AS channel_id,
                CASE
                    WHEN ec.extc_code IN (105,106) AND cm.process_type='Offer' THEN -1
                    WHEN ec.extc_code IN (76,81,87,88,104,116) AND cm.process_type='Offer' THEN -1
                    ELSE CASE WHEN cm.extc_code IN (105,106) THEN -1 ELSE COALESCE(cm.extc_code,-1) END
                END AS legal_channel_id,
                cu.customer_channel AS customer_channel,
                CAST(NULL AS SMALLINT) AS final_bucket,
                cea.PACK_ASSIGN_DATE
            FROM case_master cm
            INNER JOIN customer cu ON cu.cust_code = cm.cust_code
            LEFT JOIN employees emp ON emp.emp_code = cm.emp_code_assignment
            LEFT JOIN assignment_queue_setup aqs ON aqs.queue_desc = cm.que_desc
            LEFT JOIN external_companies ec ON LOWER(cu.customer_channel) = LOWER(ec.EXTC_COMMENT)
            LEFT JOIN case_external_assignment_agg cea ON cea.CASE_CODE = cm.case_code AND cea.VEND_CODE = cm.vend_code
            WHERE cm.vend_code = 13 AND cm.case_relation_type_code = 2090 AND cm.case_currently_open_flag = true
        """)

        # Orbit post-update
        orbit = orbit.withColumn("legal_channel_id",
            when((col("assignment_process") == "Fulfilled Settlment / Paid Off") & (col("legal_channel_id") != -1), lit(-1))
            .otherwise(col("legal_channel_id"))
        )

        # ================== LIGHT (vend_code=20) ==================
        light = self.spark.sql(f"""
            SELECT DISTINCT
                cm.vend_code, cm.debt_code, cm.case_code, cm.que_desc,
                cu.Segment, cm.Extra_Segment, cm.process_type, cm.emp_code_assignment,
                cm.extc_code,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN 'DNC_Offer'
                    WHEN cm.process_type ILIKE '%offer%' THEN 'LF_Offer'
                    WHEN cm.process_type ILIKE '%Collection%' THEN 'Collection'
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN (cu.Segment ILIKE 'Wholesale%' OR cu.Segment ILIKE 'Stated Guaranteed%' OR cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L3869%' OR cu.Segment ILIKE 'L4605%') AND cm.emp_code_assignment IS NULL THEN 'Passive_Segment'
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN 'Passive_No Segment'
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN aqs.assignment_mapping
                    WHEN cm.que_desc IN ('Light Offer CA & Jud Assign','Light Offer CA','Light Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN 'CA Dialer'
                    ELSE 'Unhandled'
                END AS assignment_process,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1
                    WHEN cm.process_type ILIKE '%offer%' THEN -1
                    WHEN cm.process_type ILIKE '%Collection%' THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN cm.emp_code_assignment
                    ELSE -1
                END AS agent_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1
                    WHEN cm.process_type ILIKE '%offer%' THEN -1
                    WHEN cm.process_type ILIKE '%Collection%' THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.team_id
                    ELSE -1
                END AS team_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN CASE WHEN cm.process_type ILIKE 'Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.process_type ILIKE '%offer%' THEN COALESCE(cm.extc_code,-1)
                    WHEN cm.process_type ILIKE '%Collection%' THEN CASE WHEN cm.process_type ILIKE 'Collection%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.dep_code
                    WHEN (cu.Segment ILIKE 'Wholesale%' OR cu.Segment ILIKE 'Stated Guaranteed%' OR cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L3869%' OR cu.Segment ILIKE 'L4605%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND NOT(aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND (aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN ec.extc_code
                    WHEN cm.que_desc IN ('Light Offer CA & Jud Assign','Light Offer CA','Light Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN {channel_collection_agents}
                    ELSE -1
                END AS channel_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1
                    WHEN cm.process_type ILIKE '%offer%' THEN -1
                    WHEN cm.process_type ILIKE '%Collection%' THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN (cu.Segment ILIKE 'Wholesale%' OR cu.Segment ILIKE 'Stated Guaranteed%' OR cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L3869%' OR cu.Segment ILIKE 'L4605%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.que_desc IN ('Light Offer CA & Jud Assign','Light Offer CA','Light Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    ELSE -1
                END AS legal_channel_id,
                cu.customer_channel AS customer_channel,
                CAST(NULL AS SMALLINT) AS final_bucket,
                cea.PACK_ASSIGN_DATE
            FROM case_master cm
            INNER JOIN customer cu ON cu.cust_code = cm.cust_code
            LEFT JOIN employees emp ON emp.emp_code = cm.emp_code_assignment
            LEFT JOIN assignment_queue_setup aqs ON aqs.queue_desc = cm.que_desc
            LEFT JOIN external_companies ec ON LOWER(cu.customer_channel) = LOWER(ec.EXTC_COMMENT)
            LEFT JOIN case_external_assignment_agg cea ON cea.CASE_CODE = cm.case_code AND cea.VEND_CODE = cm.vend_code
            WHERE cm.vend_code = 20 AND cm.case_relation_type_code = 2090 AND cm.case_currently_open_flag = true
        """)

        # ================== CELL (vend_code=30) ==================
        cell = self.spark.sql(f"""
            SELECT DISTINCT
                cm.vend_code, cm.debt_code, cm.case_code, cm.que_desc,
                cu.Segment, cm.Extra_Segment, cm.process_type, cm.emp_code_assignment,
                cm.extc_code,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN 'DNC_Offer'
                    WHEN cm.process_type ILIKE '%offer%' THEN 'LF_Offer'
                    WHEN cm.process_type ILIKE '%Collection%' THEN 'Collection'
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN (cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L4605%' OR cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN 'Passive_Segment'
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN 'Passive_No Segment'
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN aqs.assignment_mapping
                    WHEN cm.que_desc IN ('Cell Offer CA & Jud Assign','Cell Offer CA','Cell Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN 'CA Dialer'
                    ELSE 'Unhandled'
                END AS assignment_process,
                CASE WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1 WHEN cm.process_type ILIKE '%offer%' THEN -1 WHEN cm.process_type ILIKE '%Collection%' THEN -1 WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN cm.emp_code_assignment ELSE -1 END AS agent_id,
                CASE WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1 WHEN cm.process_type ILIKE '%offer%' THEN -1 WHEN cm.process_type ILIKE '%Collection%' THEN -1 WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.team_id ELSE -1 END AS team_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN CASE WHEN cm.process_type ILIKE 'Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.process_type ILIKE '%offer%' THEN COALESCE(cm.extc_code,-1)
                    WHEN cm.process_type ILIKE '%Collection%' THEN CASE WHEN cm.process_type ILIKE 'Collection%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.dep_code
                    WHEN (cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L4605%' OR cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND NOT(aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND (aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN ec.extc_code
                    WHEN cm.que_desc IN ('Cell Offer CA & Jud Assign','Cell Offer CA','Cell Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN {channel_collection_agents}
                    ELSE -1
                END AS channel_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1
                    WHEN cm.process_type ILIKE '%offer%' THEN -1
                    WHEN cm.process_type ILIKE '%Collection%' THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN (cu.Segment ILIKE '%1st Lien%' OR cu.Segment ILIKE 'L4605%' OR cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.que_desc IN ('Cell Offer CA & Jud Assign','Cell Offer CA','Cell Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    ELSE -1
                END AS legal_channel_id,
                cu.customer_channel AS customer_channel,
                CAST(NULL AS SMALLINT) AS final_bucket,
                cea.PACK_ASSIGN_DATE
            FROM case_master cm
            INNER JOIN customer cu ON cu.cust_code = cm.cust_code
            LEFT JOIN employees emp ON emp.emp_code = cm.emp_code_assignment
            LEFT JOIN assignment_queue_setup aqs ON aqs.queue_desc = cm.que_desc
            LEFT JOIN external_companies ec ON LOWER(cu.customer_channel) = LOWER(ec.EXTC_COMMENT)
            LEFT JOIN case_external_assignment_agg cea ON cea.CASE_CODE = cm.case_code AND cea.VEND_CODE = cm.vend_code
            WHERE cm.vend_code = 30 AND cm.case_relation_type_code = 2090 AND cm.case_currently_open_flag = true
        """)

        # ================== PEARL (vend_code=34) ==================
        pearl = self.spark.sql(f"""
            SELECT DISTINCT
                cm.vend_code, cm.debt_code, cm.case_code, cm.que_desc,
                cu.Segment, cm.Extra_Segment, cm.process_type, cm.emp_code_assignment,
                cm.extc_code,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN 'DNC_Offer'
                    WHEN cm.process_type ILIKE '%offer%' THEN 'LF_Offer'
                    WHEN cm.process_type ILIKE '%Collection%' THEN 'Collection'
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN 'CA/FA/Sme Officer'
                    WHEN (cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN 'Passive_Segment'
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN 'Passive_No Segment'
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN aqs.assignment_mapping
                    WHEN (cm.que_desc ILIKE '%Offer CA & Jud Assign' OR cm.que_desc ILIKE '%Offer CA' OR cm.que_desc ILIKE '%Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN 'CA Dialer'
                    ELSE 'Unhandled'
                END AS assignment_process,
                CASE WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1 WHEN cm.process_type ILIKE '%offer%' THEN -1 WHEN cm.process_type ILIKE '%Collection%' THEN -1 WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN cm.emp_code_assignment ELSE -1 END AS agent_id,
                CASE WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1 WHEN cm.process_type ILIKE '%offer%' THEN -1 WHEN cm.process_type ILIKE '%Collection%' THEN -1 WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.team_id ELSE -1 END AS team_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN CASE WHEN cm.process_type ILIKE 'Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.process_type ILIKE '%offer%' THEN COALESCE(cm.extc_code,-1)
                    WHEN cm.process_type ILIKE '%Collection%' THEN CASE WHEN cm.process_type ILIKE 'Collection%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN emp.dep_code
                    WHEN (cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND NOT(aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN CASE WHEN cm.process_type IS NOT NULL AND (cm.process_type ILIKE 'Collection%' OR cm.process_type ILIKE '%Offer%') THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL AND (aqs.queue_desc ILIKE '%Fulfilled%' OR aqs.queue_desc ILIKE '%Paid_Off%' OR aqs.queue_desc ILIKE '%Εξοφλήσεις%') THEN ec.extc_code
                    WHEN (cm.que_desc ILIKE '%Offer CA & Jud Assign' OR cm.que_desc ILIKE '%Offer CA' OR cm.que_desc ILIKE '%Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN {channel_collection_agents}
                    ELSE -1
                END AS channel_id,
                CASE
                    WHEN cm.process_type ILIKE '%offer%' AND cm.extc_code IN (105,106) THEN -1
                    WHEN cm.process_type ILIKE '%offer%' THEN -1
                    WHEN cm.process_type ILIKE '%Collection%' THEN -1
                    WHEN cm.emp_code_assignment IS NOT NULL AND emp.team_name IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN (cu.Segment ILIKE 'L3869 Accepted%' OR cu.Segment ILIKE 'L3869 Pending%' OR cu.Segment ILIKE 'L3869 Rejected >10k%') AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cu.Segment IS NULL AND cm.emp_code_assignment IS NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN cm.emp_code_assignment IS NULL AND emp.team_name IS NULL AND aqs.queue_desc IS NOT NULL THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    WHEN (cm.que_desc ILIKE '%Offer CA & Jud Assign' OR cm.que_desc ILIKE '%Offer CA' OR cm.que_desc ILIKE '%Offer VIP CA') AND cm.emp_code_assignment IS NULL AND (cm.process_type IS NULL OR cm.process_type ILIKE '%Judicial%') THEN CASE WHEN cm.process_type IS NOT NULL AND cm.process_type NOT ILIKE 'Collection%' AND cm.process_type NOT ILIKE '%Offer%' THEN COALESCE(cm.extc_code,-1) ELSE -1 END
                    ELSE -1
                END AS legal_channel_id,
                cu.customer_channel AS customer_channel,
                CAST(NULL AS SMALLINT) AS final_bucket,
                cea.PACK_ASSIGN_DATE
            FROM case_master cm
            INNER JOIN customer cu ON cu.cust_code = cm.cust_code
            LEFT JOIN employees emp ON emp.emp_code = cm.emp_code_assignment
            LEFT JOIN assignment_queue_setup aqs ON aqs.queue_desc = cm.que_desc
            LEFT JOIN external_companies ec ON LOWER(cu.customer_channel) = LOWER(ec.EXTC_COMMENT)
            LEFT JOIN case_external_assignment_agg cea ON cea.CASE_CODE = cm.case_code AND cea.VEND_CODE = cm.vend_code
            WHERE cm.vend_code = 34 AND cm.case_relation_type_code = 2090 AND cm.case_currently_open_flag = true
        """)

        # ================== UNION ==================
        combined_df = (
            mercury
            .unionByName(orbit, allowMissingColumns=True)
            .unionByName(light, allowMissingColumns=True)
            .unionByName(cell, allowMissingColumns=True)
            .unionByName(pearl, allowMissingColumns=True)
            .dropDuplicates()
        )

        combined_df = combined_df.withColumn("update_date", current_date())
        combined_df = combined_df.withColumn("BusinessDate", lit(self.run_date).cast("date"))
        combined_df = combined_df.withColumn("LoadingDateTime", from_utc_timestamp(current_timestamp(), "Europe/Athens"))

        # Select columns in final order
        combined_df = combined_df.select(
            "vend_code",
            "debt_code",
            "case_code",
            "que_desc",
            "Segment",
            "Extra_Segment",
            "process_type",
            "emp_code_assignment",
            "extc_code",
            "assignment_process",
            "agent_id",
            "team_id",
            "channel_id",
            "legal_channel_id",
            "update_date",
            "customer_channel",
            "final_bucket",
            "PACK_ASSIGN_DATE",
            "BusinessDate",
            "LoadingDateTime",
        )

        return combined_df
