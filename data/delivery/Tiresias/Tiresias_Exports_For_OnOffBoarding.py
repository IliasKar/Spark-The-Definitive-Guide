"""
Tiresias Export Module
=====================
Produces Announcement and Acceptance files for Tiresias
during portfolio migrations (onboarding/offboarding/internal).

Usage (Databricks notebook):
    from Tiresias_Exports_For_OnOffBoarding import TiresiasExport

    export = TiresiasExport(
        spark=spark,
        migration_type="internal",        # "onboarding" | "offboarding" | "internal"
        portfolio_filter="ASTR%",          # LIKE filter for PortfolioName
        exposure_prefix="AST",             # prefix to strip from ExposureKey
        assignment_date="2026-03-09",      # migration date (from ref.tiresias_portfolio)
        portfolio_name="ASTRAEA PORTFOLIO RETAIL",
        edadp_from="CPA",                  # servicer code FROM
        edadp_to="CPA",                    # servicer code TO
        output_dir="/Workspace/Shared/tiresias_exports/"
    )

    export.run()
"""

import logging
import sys
from pyspark.sql import SparkSession, DataFrame

# Import environment utilities
sys.path.append("../aws_utils")
import aws_utils

# Get Environment Prefix (e.g. "p", "qa", "d")
env = aws_utils.get_env_prefix()


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logger = logging.getLogger("tiresias_export")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# Main Export Class
# ---------------------------------------------------------------------------

class TiresiasExport:
    """Orchestrates the full Tiresias export pipeline."""

    def __init__(self, spark: SparkSession, migration_type: str, portfolio_filter: str,
                 exposure_prefix: str, assignment_date: str, portfolio_name: str,
                 edadp_from: str, edadp_to: str,
                 output_dir: str = "/Workspace/Shared/tiresias_exports/"):

        self.spark = spark
        self.migration_type = migration_type.lower()
        self.portfolio_filter = portfolio_filter
        self.exposure_prefix = exposure_prefix
        self.assignment_date = assignment_date
        self.portfolio_name = portfolio_name
        self.edadp_from = edadp_from
        self.edadp_to = edadp_to
        self.output_dir = output_dir

        # Derived fields
        self.assignment_date_compact = self.assignment_date.replace("-", "")
        self.prefix_len = len(self.exposure_prefix) + 1

        # Which files to produce based on migration type
        if self.migration_type == "onboarding":
            self.produces_announcement = False
            self.produces_acceptance = True
        elif self.migration_type == "offboarding":
            self.produces_announcement = True
            self.produces_acceptance = False
        elif self.migration_type == "internal":
            self.produces_announcement = True
            self.produces_acceptance = True
        else:
            raise ValueError(f"Invalid migration_type: {self.migration_type}. "
                             "Use 'onboarding', 'offboarding', or 'internal'.")

        # Table references (dynamic based on environment)
        self.tbl_exposure = f"`{env}-cdh`.derivative.dexposure"
        self.tbl_portfolio = f"`{env}-cdh`.primary.portfolio"
        self.tbl_export_business = f"`{env}-cdh-delivery`.tiresias.export_business"
        self.tbl_export_consumer = f"`{env}-cdh-delivery`.tiresias.export_consumer"
        self.tbl_loan = f"`{env}-cdh-delivery`.tiresias.loan"
        self.tbl_loan_customer = f"`{env}-cdh-delivery`.tiresias.loan_customer"
        self.tbl_customer = f"`{env}-cdh-delivery`.tiresias.customer"
        self.tbl_subsidy = f"`{env}-cdh`.landed.profits_lns_ext_subsidy_hd"

        # Validate
        self._validate()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self):
        """Execute the full export pipeline."""
        logger.info(f"Starting Tiresias export: type={self.migration_type}, "
                    f"portfolio_filter={self.portfolio_filter}")

        # Step 0: Create dynamic portfolio reference view
        self._create_portfolio_view()

        # Step 1: Build perimeter
        self._create_acc_perimeter()
        perimeter_count = self.spark.sql("SELECT COUNT(*) AS cnt FROM ACCPerimeter").first().cnt
        logger.info(f"ACCPerimeter: {perimeter_count} records")

        # Step 2: Build final perimeter (join with export tables)
        self._create_perimeter()
        final_count = self.spark.sql(
            "SELECT COUNT(DISTINCT `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`) AS cnt FROM Perimeter"
        ).first().cnt
        logger.info(f"Perimeter (final): {final_count} distinct accounts")

        # Step 3: Validation checks
        missing_accounts = self._get_missing_accounts()
        if len(missing_accounts) > 0:
            self._diagnose_missing_accounts(missing_accounts)
        else:
            logger.info("All accounts matched. No missing records.")

        # Step 4: Produce files based on migration type
        if self.produces_announcement:
            self._create_announcement_view()
            self._export_announcement()

        if self.produces_acceptance:
            self._export_acceptance()

        logger.info("Tiresias export completed successfully.")

    # ------------------------------------------------------------------
    # Step 0: Portfolio Reference (dynamic)
    # ------------------------------------------------------------------

    def _create_portfolio_view(self):
        """Build portfolio reference dynamically from primary.portfolio."""
        sql = f"""
        SELECT
            PortfolioId,
            PortfolioName,
            TiresiasCode,
            cast(AssignmentDate AS DATE) AS AssignmentDate
        FROM {self.tbl_portfolio} p
        WHERE tiresiascode IS NOT NULL
            AND BusinessDate = '{self.assignment_date}'
        """
        self.spark.sql(sql).createOrReplaceTempView("TiresiasPortfolio")
        logger.info("TiresiasPortfolio view created dynamically")

    # ------------------------------------------------------------------
    # Step 1: ACCPerimeter
    # ------------------------------------------------------------------

    def _create_acc_perimeter(self):
        sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW ACCPerimeter AS
        WITH old_portfolio AS (
            SELECT
                substring(e.ExposureKey, {self.prefix_len}, 99) AS AccountNumber,
                e.PortfolioName,
                p.PortfolioId AS OLDPortfolioID,
                p.TiresiasCode AS OLDPortfolioCode,
                ROW_NUMBER() OVER (
                    PARTITION BY substring(e.ExposureKey, {self.prefix_len}, 99)
                    ORDER BY e.BusinessDate DESC
                ) AS rn
            FROM {self.tbl_exposure} e
            JOIN TiresiasPortfolio p
                ON p.PortfolioName = e.PortfolioName
            WHERE e.PortfolioName <> '{self.portfolio_name}'
        )
        SELECT
            substring(e.ExposureKey, {self.prefix_len}, 99) AS AccountNumber,
            e.ExposureKey AS assetkey,
            e.AgreementKey AS AgreementNumber,
            p.PortfolioId AS NEWPortfolioID,
            p.TiresiasCode AS NEWPortfolioCode,
            e_old.OLDPortfolioID,
            e_old.OLDPortfolioCode
        FROM {self.tbl_exposure} e
        INNER JOIN TiresiasPortfolio p
            ON p.PortfolioName = e.PortfolioName
        LEFT JOIN old_portfolio e_old
            ON e_old.AccountNumber = substring(e.ExposureKey, {self.prefix_len}, 99)
            AND e_old.rn = 1
        WHERE e.BusinessDate = p.AssignmentDate
            AND e.IsActiveFlag = TRUE
            AND p.PortfolioName LIKE '{self.portfolio_filter}'
            AND e.ExposureKey LIKE '{self.exposure_prefix}%'
        """
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Step 2: Final Perimeter
    # ------------------------------------------------------------------

    def _create_perimeter(self):
        sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW Perimeter AS
        -- Business
        SELECT
            h.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`,
            -- h.`ΗΜ_ΤΕΛΕΥΤΑΙΑΣ_ΕΝΗΜΕΡΩΣΗΣ`,
            h.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΔΑΝΕΙΟΥ`,
            h.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΟΦΕΙΛΕΤΗ`,
            h.`ΑΦΜ`,
            -- h.`ΑΔΤ`,
            -- h.`ΗΜ_ΕΚΔΟΣΗΣ_ΑΔΤ`,
            substring(
                coalesce(h.`ΚΩΔΙΚΟΣ_ΚΟΙΝΗΣ_ΣΥΝΔΕΔΕΜΕΝΗΣ_ΡΥΘΜΙΣΗΣ`, a.AgreementNumber), 1, 15
            ) AS AgrementNumberTir,
            a.NEWPortfolioCode,
            a.OLDPortfolioCode,
            a.AgreementNumber,
            'B' AS fileType,
            'M' AS InfoType
        FROM ACCPerimeter A
        JOIN {self.tbl_export_business} H
            ON h.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` = a.AccountNumber

        UNION ALL

        -- Consumer
        SELECT
            h.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`,
            -- h.`ΗΜ_ΤΕΛΕΥΤΑΙΑΣ_ΕΝΗΜΕΡΩΣΗΣ`,
            h.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΔΑΝΕΙΟΥ`,
            h.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΟΦΕΙΛΕΤΗ`,
            h.`ΑΦΜ`,
            -- h.`ΑΔΤ`,
            -- h.`ΗΜ_ΕΚΔΟΣΗΣ_ΑΔΤ`,
            substring(
                coalesce(h.`ΚΩΔΙΚΟΣ_ΚΟΙΝΗΣ_ΣΥΝΔΕΔΕΜΕΝΗΣ_ΡΥΘΜΙΣΗΣ`, a.AgreementNumber), 1, 15
            ) AS AgrementNumberTir,
            a.NEWPortfolioCode,
            a.OLDPortfolioCode,
            a.AgreementNumber,
            'C' AS fileType,
            'D' AS InfoType
        FROM ACCPerimeter A
        JOIN {self.tbl_export_consumer} H
            ON h.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` = a.AccountNumber
        """
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Step 3: Validation
    # ------------------------------------------------------------------

    def _get_missing_accounts(self) -> list:
        """Returns list of accounts in ACCPerimeter but missing from Perimeter."""
        sql = """
        SELECT DISTINCT AccountNumber FROM ACCPerimeter
        EXCEPT
        SELECT DISTINCT `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` FROM Perimeter
        """
        rows = self.spark.sql(sql).collect()
        return [row.AccountNumber for row in rows]

    def _diagnose_missing_accounts(self, missing_accounts: list):
        """
        Diagnose all missing accounts, categorize them, log a summary,
        and produce a detailed CSV report file (Warnings.csv).
        """
        import os

        # Collect diagnostics - join with ACCPerimeter to get assetkey for matching
        missing_df = self.spark.createDataFrame(
            [(acc,) for acc in missing_accounts], ["AccountNumber"]
        )
        missing_df.createOrReplaceTempView("MissingAccountsList")

        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW MissingAccounts AS
            SELECT DISTINCT a.AccountNumber, a.assetkey
            FROM ACCPerimeter a
            INNER JOIN MissingAccountsList m ON m.AccountNumber = a.AccountNumber
        """)

        diagnostics_sql = f"""
        SELECT
            m.AccountNumber,
            COALESCE(eb.cnt, 0) AS export_business,
            COALESCE(ec.cnt, 0) AS export_consumer,
            COALESCE(ln.cnt, 0) AS loan,
            COALESCE(lc.cnt, 0) AS loan_customer,
            COALESCE(sb.cnt, 0) AS subsidy_excluded
        FROM MissingAccounts m
        LEFT JOIN (
            SELECT `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` AS acc, COUNT(*) AS cnt
            FROM {self.tbl_export_business}
            GROUP BY `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`
        ) eb ON eb.acc = m.assetkey
        LEFT JOIN (
            SELECT `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` AS acc, COUNT(*) AS cnt
            FROM {self.tbl_export_consumer}
            GROUP BY `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`
        ) ec ON ec.acc = m.assetkey
        LEFT JOIN (
            SELECT account_number AS acc, COUNT(*) AS cnt
            FROM {self.tbl_loan}
            GROUP BY account_number
        ) ln ON ln.acc = m.assetkey
        LEFT JOIN (
            SELECT account_number AS acc, COUNT(*) AS cnt
            FROM {self.tbl_loan_customer}
            GROUP BY account_number
        ) lc ON lc.acc = m.assetkey
        LEFT JOIN (
            SELECT account_number AS acc, COUNT(*) AS cnt
            FROM {self.tbl_subsidy}
            GROUP BY account_number
        ) sb ON sb.acc = m.assetkey
        """
        diag_rows = self.spark.sql(diagnostics_sql).collect()

        # Categorize
        not_found_anywhere = []
        in_consumer_only = []
        in_business_only = []
        in_loan_but_not_export = []
        other = []

        for row in diag_rows:
            acc = row.AccountNumber
            eb, ec, ln, lc = row.export_business, row.export_consumer, row.loan, row.loan_customer

            if eb == 0 and ec == 0 and ln == 0 and lc == 0:
                not_found_anywhere.append(acc)
            elif ec > 0 and eb == 0:
                in_consumer_only.append(acc)
            elif eb > 0 and ec == 0:
                in_business_only.append(acc)
            elif ln > 0 and eb == 0 and ec == 0:
                in_loan_but_not_export.append(acc)
            else:
                other.append(acc)

        # Log summary
        total = len(missing_accounts)
        logger.warning(f"{total} accounts in ACCPerimeter but NOT in Perimeter:")
        if not_found_anywhere:
            logger.warning(f"  - {len(not_found_anywhere)} accounts: "
                           "Not found in any Tiresias table (new/not yet processed)")
        if in_consumer_only:
            logger.warning(f"  - {len(in_consumer_only)} accounts: "
                           "Found in export_consumer but NOT in export_business")
        if in_business_only:
            logger.warning(f"  - {len(in_business_only)} accounts: "
                           "Found in export_business but NOT in export_consumer")
        if in_loan_but_not_export:
            logger.warning(f"  - {len(in_loan_but_not_export)} accounts: "
                           "Found in loan/loan_customer but NOT in export tables")
        if other:
            logger.warning(f"  - {len(other)} accounts: "
                           "Found in multiple tables but join still failed")

        # Write detailed report CSV
        report_path = os.path.join(self.output_dir, "Warnings.csv")
        os.makedirs(self.output_dir, exist_ok=True)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("AccountNumber,Category,export_business,export_consumer,loan,loan_customer,subsidy_excluded\n")
            for row in diag_rows:
                acc = row.AccountNumber
                eb, ec, ln, lc, sb = (row.export_business, row.export_consumer,
                                      row.loan, row.loan_customer, row.subsidy_excluded)

                if acc in not_found_anywhere:
                    cat = "NOT_IN_ANY_TABLE"
                elif acc in in_consumer_only:
                    cat = "CONSUMER_ONLY"
                elif acc in in_business_only:
                    cat = "BUSINESS_ONLY"
                elif acc in in_loan_but_not_export:
                    cat = "LOAN_NO_EXPORT"
                else:
                    cat = "OTHER"

                f.write(f"{acc},{cat},{eb},{ec},{ln},{lc},{sb}\n")

        logger.info(f"Detailed missing accounts report: {report_path}")

    # ------------------------------------------------------------------
    # Step 4a: Announcement File
    # ------------------------------------------------------------------

    def _create_announcement_view(self):
        sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW AnnouncementFileExp AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY p.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`) AS Id,
            '{self.edadp_from}'                              AS `ΚΩΔΙΚΟΣ ΔΙΑΧΕΙΡΙΣΤΗ ΑΠΟ`,
            p.OLDPortfolioCode                               AS `ΚΩΔΙΚΟΣ FUND/SPV ΑΠΟ`,
            p.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`               AS `ΚΩΔΙΚΟΣ ΧΟΡΗΓΗΣΗΣ ΑΠΟ`,
            p.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΔΑΝΕΙΟΥ`                       AS `ΧΑΡΑΚΤΗΡΙΣΜΟΣ ΧΟΡΗΓΗΣΗΣ`,
            p.InfoType                                       AS `ΤΥΠΟΣ ΠΛΗΡΟΦΟΡΙΑΣ`,
            '{self.edadp_to}'                                AS `ΚΩΔΙΚΟΣ ΔΙΑΧΕΙΡΙΣΤΗ ΣΕ`,
            p.NEWPortfolioCode                               AS `ΚΩΔΙΚΟΣ FUND/SPV ΣΕ`,
            p.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`               AS `ΚΩΔΙΚΟΣ ΧΟΡΗΓΗΣΗΣ ΣΕ`,
            '{self.portfolio_name}'                          AS `ΟΝΟΜΑ ΧΑΡΤΟΦΥΛΑΚΙΟΥ`,
            '{self.assignment_date_compact}'                 AS `ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΑΒΙΒΑΣΗΣ`,
            COALESCE(p.`ΑΦΜ`, '000000000')                  AS `ΑΦΜ ΟΦΕΙΛΕΤΗ`,
            p.`ΧΑΡΑΚΤΗΡΙΣΜΟΣ_ΟΦΕΙΛΕΤΗ`                      AS `ΧΑΡΑΚΤΗΡΙΣΜΟΣ ΟΦΕΙΛΕΤΗ`,
            substring(p.AgreementNumber, {self.prefix_len}, 99) AS `ΑΡΙΘΜΟΣ ΣΥΜΒΑΣΗΣ ΟΦΕΙΛΗΣ`,
            p.`ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ`               AS `ΑΡΙΘΜΟΣ ΛΟΓΑΡΙΑΣΜΟΥ ΟΦΕΙΛΗΣ`,
            'Ο' AS `ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4469`,
            ''  AS `ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ1`,
            'Ο' AS `ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4605`,
            ''  AS `ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ2`,
            'Ο' AS `ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 3869`,
            ''  AS `ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ3`,
            'Ο' AS `ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4738`,
            ''  AS `ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ4`,
            p.fileType AS dataFile
        FROM Perimeter p
        """
        self.spark.sql(sql)

    def _export_announcement(self):
        """Generate and write Announcement files (Business + Consumer)."""
        for file_type_code, file_type_label in [("B", "MME"), ("C", "SSX")]:
            df = self._build_announcement_df(file_type_code)
            count = df.count()
            if count == 0:
                logger.info(f"No {file_type_label} announcement records. Skipping.")
                continue

            filename = self._build_filename("Announcement", file_type_label)
            self._write_file(df, filename)
            logger.info(f"Announcement {file_type_label}: {count} lines -> {filename}")

    def _build_announcement_df(self, file_type: str) -> DataFrame:
        """Build the CONCAT query for announcement file."""
        sql = f"""
        SELECT CONCAT(
            RIGHT(CONCAT('00000', CAST(Id AS STRING)), 6), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ ΔΙΑΧΕΙΡΙΣΤΗ ΑΠΟ`, SPACE(3)), 3), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ FUND/SPV ΑΠΟ`, SPACE(3)), 3), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ ΧΟΡΗΓΗΣΗΣ ΑΠΟ`, SPACE(16)), 16), ';',
            LEFT(CONCAT(`ΧΑΡΑΚΤΗΡΙΣΜΟΣ ΧΟΡΗΓΗΣΗΣ`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΤΥΠΟΣ ΠΛΗΡΟΦΟΡΙΑΣ`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ ΔΙΑΧΕΙΡΙΣΤΗ ΣΕ`, SPACE(3)), 3), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ FUND/SPV ΣΕ`, SPACE(3)), 3), ';',
            LEFT(CONCAT(`ΚΩΔΙΚΟΣ ΧΟΡΗΓΗΣΗΣ ΣΕ`, SPACE(16)), 16), ';',
            LEFT(CONCAT(`ΟΝΟΜΑ ΧΑΡΤΟΦΥΛΑΚΙΟΥ`, SPACE(20)), 20), ';',
            LEFT(CONCAT(`ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΑΒΙΒΑΣΗΣ`, SPACE(8)), 8), ';',
            LEFT(CONCAT(`ΑΦΜ ΟΦΕΙΛΕΤΗ`, SPACE(9)), 9), ';',
            LEFT(CONCAT(`ΧΑΡΑΚΤΗΡΙΣΜΟΣ ΟΦΕΙΛΕΤΗ`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΑΡΙΘΜΟΣ ΣΥΜΒΑΣΗΣ ΟΦΕΙΛΗΣ`, SPACE(50)), 50), ';',
            LEFT(CONCAT(`ΑΡΙΘΜΟΣ ΛΟΓΑΡΙΑΣΜΟΥ ΟΦΕΙΛΗΣ`, SPACE(50)), 50), ';',
            LEFT(CONCAT(`ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4469`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ1`, SPACE(50)), 50), ';',
            LEFT(CONCAT(`ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4605`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ2`, SPACE(50)), 50), ';',
            LEFT(CONCAT(`ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 3869`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ3`, SPACE(50)), 50), ';',
            LEFT(CONCAT(`ΥΠΑΓΩΓΗ ΣΤΟ ΝΟΜΟ 4738`, SPACE(1)), 1), ';',
            LEFT(CONCAT(`ΤΑΥΤΟΤΗΤΑ ΟΦΕΙΛΗΣ ΜΗΝΥΜΑΤΟΣ Γ4`, SPACE(50)), 50), ';',
            SPACE(19)
        ) AS line
        FROM AnnouncementFileExp
        WHERE dataFile = '{file_type}'
        """
        return self.spark.sql(sql).filter("line IS NOT NULL")

    # ------------------------------------------------------------------
    # Step 4b: Acceptance File
    # ------------------------------------------------------------------

    def _export_acceptance(self):
        """Generate and write Acceptance files (Business + Consumer)."""
        for file_type_code, file_type_label in [("B", "MME"), ("C", "SSX")]:
            df = self._build_acceptance_df(file_type_code)
            count = df.count()
            if count == 0:
                logger.info(f"No {file_type_label} acceptance records. Skipping.")
                continue

            filename = self._build_filename("AcceptanceFile", file_type_label)
            self._write_file(df, filename)
            logger.info(f"Acceptance {file_type_label}: {count} lines -> {filename}")

    def _build_acceptance_df(self, file_type: str) -> DataFrame:
        """Build the CONCAT query for acceptance file."""
        sql = f"""
        WITH d AS (
            SELECT DISTINCT
                `ΚΩΔΙΚΟΣ_ΛΟΓΑΡΙΑΣΜΟΥ_ΧΟΡΗΓΗΣΗΣ` AS AccountNumber,
                OLDPortfolioCode
            FROM Perimeter
            WHERE fileType = '{file_type}'
        ),
        f AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY AccountNumber, OLDPortfolioCode) AS Numbering,
                AccountNumber,
                OLDPortfolioCode
            FROM d
        )
        SELECT CONCAT(
            LPAD(TRIM(CAST(Numbering AS STRING)), 6, '0'), ';',
            COALESCE(OLDPortfolioCode, ''), ';',
            RPAD(TRIM(AccountNumber), 16, ' '), ';',
            'CPA', ';',
            RPAD(TRIM(AccountNumber), 16, ' '), ';'
        ) AS line
        FROM f
        """
        return self.spark.sql(sql).filter("line IS NOT NULL")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _build_filename(self, file_prefix: str, file_type_label: str) -> str:
        """Build filename like: AcceptanceFile_SSX_ABC_KYM_20260622.txt"""
        return (f"{file_prefix}_{file_type_label}"
                f"_{self.edadp_to}_{self.exposure_prefix}_{self.assignment_date_compact}.txt")

    def _write_file(self, df: DataFrame, filename: str):
        """Write DataFrame (single 'line' column) to text file."""
        import os
        output_path = os.path.join(self.output_dir, filename)
        os.makedirs(self.output_dir, exist_ok=True)

        pdf = df.toPandas()
        pdf = pdf[pdf["line"].notna()]

        with open(output_path, "w", encoding="utf-8") as f:
            for line in pdf["line"]:
                f.write(line + "\n")

        logger.info(f"Written {len(pdf)} lines to {output_path}")

    def _validate(self):
        """Basic validation of inputs."""
        if not self.assignment_date:
            raise ValueError("assignment_date is required")
        if not self.portfolio_filter:
            raise ValueError("portfolio_filter is required")
        if not self.edadp_from or not self.edadp_to:
            raise ValueError("edadp_from and edadp_to are required")

        parts = self.assignment_date.split("-")
        if len(parts) != 3 or len(parts[0]) != 4:
            raise ValueError(
                f"assignment_date must be YYYY-MM-DD, got: {self.assignment_date}"
            )

        logger.info(f"Config validated: {self.migration_type} migration, "
                    f"date={self.assignment_date}")


# ---------------------------------------------------------------------------
# CLI Entry Point (for Databricks Python Script Task)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiresias Export - OnOffBoarding")
    parser.add_argument("--migration_type", required=True,
                        choices=["onboarding", "offboarding", "internal"],
                        help="Type of migration")
    parser.add_argument("--portfolio_filter", required=True,
                        help="LIKE filter for PortfolioName, e.g. 'KYM%%'")
    parser.add_argument("--exposure_prefix", required=True,
                        help="Prefix to strip from ExposureKey, e.g. 'KYM'")
    parser.add_argument("--assignment_date", required=True,
                        help="Migration date in YYYY-MM-DD format")
    parser.add_argument("--portfolio_name", required=True,
                        help="Portfolio name, e.g. 'KYMA'")
    parser.add_argument("--edadp_from", required=True,
                        help="Servicer code FROM, e.g. 'CPA'")
    parser.add_argument("--edadp_to", required=True,
                        help="Servicer code TO, e.g. 'CPA'")
    parser.add_argument("--output_dir",
                        default="/Workspace/Shared/tiresias_exports/",
                        help="Output directory for export files")

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    export = TiresiasExport(
        spark=spark,
        migration_type=args.migration_type,
        portfolio_filter=args.portfolio_filter,
        exposure_prefix=args.exposure_prefix,
        assignment_date=args.assignment_date,
        portfolio_name=args.portfolio_name,
        edadp_from=args.edadp_from,
        edadp_to=args.edadp_to,
        output_dir=args.output_dir,
    )

    export.run()
