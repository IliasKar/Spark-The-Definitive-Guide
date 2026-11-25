import re
from io import BytesIO
from datetime import datetime
from pytz import timezone
import pandas as pd
import os

def _build_filename_date(last_eom_business_date):
    """Build filename date string: YYYYMMDD_YYYYMMDDTHHmmSS"""
    stripped_date = re.sub(r"-", "", last_eom_business_date)
    now_str = datetime.now(timezone('Europe/Athens')).strftime('%Y%m%dT%H%M%S')
    return f"{stripped_date}_{now_str}"

def export_to_excel(df,excel_name,last_eom_business_date,output_dir,output_file_prefix):
    date_str = _build_filename_date(last_eom_business_date)
    output_df = df.toPandas()
    
    if not output_df.empty:
        output_buffer = BytesIO()
        with pd.ExcelWriter(output_buffer) as writer:
            output_df.to_excel(writer, index=False)

        full_export_output = os.path.join(
            output_dir, f"{output_file_prefix}_{excel_name}_{date_str}.xlsx"
        )

        with open(full_export_output, "wb") as f:
            f.write(output_buffer.getvalue())

        print(f"Results successfully written to Volume at: {full_export_output}")
    else:
        print(f"No data to write for query: {excel_name}")

def export_to_csv(df, csv_name, last_eom_business_date, output_dir, output_file_prefix):
     """
     Export Spark DataFrame to CSV, replacing existing files if already present.
     Works with very large DataFrames (no Excel row limit).
     """
     date_str = _build_filename_date(last_eom_business_date)
     output_df = df.toPandas()
     print(f"Exporting {csv_name} with shape {output_df.shape}")
 
     if output_df.empty or output_df.shape[1] == 0:
        print(f"No data to write for query: {csv_name}")
        return
 
     # File path
     full_export_output = os.path.join(
         output_dir, f"{output_file_prefix}_{csv_name}_{date_str}.csv"
     )
 
     # If file exists, remove it so we replace cleanly
     if os.path.exists(full_export_output):
        os.remove(full_export_output)
        print(f"Existing file removed: {full_export_output}")
 
     # Export to CSV
     output_df.to_csv(full_export_output, index=False)
 
     print(f"Results successfully written to: {full_export_output}")


EXCEL_MAX_ROWS = 1_000_000  # safe limit (xlsx supports 1,048,576 including header)


def export_data(df, name, last_eom_business_date, output_dir, output_file_prefix):
    """
    Export Spark DataFrame to file. Automatically chooses format:
    - xlsx if row count < 1,048,576 (Excel limit)
    - csv if row count >= 1,048,576

    Replaces export_to_excel and export_to_csv.
    """
    date_str = _build_filename_date(last_eom_business_date)
    output_df = df.toPandas()

    if output_df.empty or output_df.shape[1] == 0:
        print(f"No data to write for: {name}")
        return

    row_count = len(output_df)

    if row_count < EXCEL_MAX_ROWS:
        full_export_output = os.path.join(
            output_dir, f"{output_file_prefix}_{name}_{date_str}.xlsx"
        )
        output_buffer = BytesIO()
        with pd.ExcelWriter(output_buffer) as writer:
            output_df.to_excel(writer, index=False)
        with open(full_export_output, "wb") as f:
            f.write(output_buffer.getvalue())
    else:
        print(f"  ⚠ {name}: {row_count} rows exceeds xlsx limit. Exporting as CSV.")
        full_export_output = os.path.join(
            output_dir, f"{output_file_prefix}_{name}_{date_str}.csv"
        )
        if os.path.exists(full_export_output):
            os.remove(full_export_output)
        output_df.to_csv(full_export_output, index=False)

    print(f"Results written to: {full_export_output} ({row_count} rows)")
