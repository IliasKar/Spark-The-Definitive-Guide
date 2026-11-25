# Databricks notebook source
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, datediff, expr, sum as _sum, round as _round

# def calculate_WAL_NPV(data, IRR):
#     try:
#         IRR_value = IRR.select('Discount Rate').first()[0]
#         min_date = data.agg({"Date": "min"}).first()[0]
#         filtered_data = data.filter(col('Amount') > 0)

#         wal_df = filtered_data.withColumn(
#             'years', (datediff(col('Date'), expr(f"'{min_date}'")) + 1) / 365.0
#         ).withColumn(
#             'product', col('Amount') * col('years')
#         ).groupBy('key').agg(
#             _sum('Amount').alias('total_amount'),
#             _sum('product').alias('total_product')
#         ).withColumn(
#             'WAL', _round(col('total_product') / col('total_amount'), 3)
#         ).select('key', 'WAL')

#         npv_df = filtered_data.withColumn(
#             'months', (datediff(col('Date'), expr(f"'{min_date}'")) / 30).cast('int')
#         ).withColumn(
#             'NPV', col('Amount') / (1 + IRR_value / 12) ** col('months')
#         ).groupBy('key').agg(
#             _round(_sum('NPV'), 3).alias('NPV')
#         )
        
#         result_df = wal_df.join(npv_df, on='key', how='outer')
#         debtors = data.select('key').distinct()
#         output_table = debtors.join(result_df, on='key', how='left').na.fill(0)

#         return output_table

#     except Exception as e:
#         print(f"Calculation failed! The following error occurred:\n{e}")
#         return None


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import timedelta

def calculate_WAL_NPV(
    initial_data,  # This should be a Spark DataFrame
    portfolio_IRR  # This should also be a Spark DataFrame
) -> dict:
    """
    Calculates WAL and NPV using PySpark DataFrames.
    
    Inputs:
    1. initial_data: Spark DataFrame with columns: 'PortfolioId', 'key', 'Date', and 'Amount'.
    2. portfolio_IRR: Spark DataFrame with columns: 'PortfolioId', 'DiscountRate'.
    
    Returns:
    output_table: Dictionary where keys are PortfolioId, and values are Spark DataFrames with calculated KPIs: 'key', 'WAL', 'NPV'.
    """
    try:
        output_table = {}

        # Collect portfolio IRRs into a dictionary for lookup
        irr_dict = {row['PortfolioId']: row['DiscountRate'] for row in portfolio_IRR.collect()}

        unique_portfolio_ids = initial_data.select("PortfolioId").distinct().collect()

        # Loop over each PortfolioId in portfolio_IRR
        for row in unique_portfolio_ids:
            PortfolioId = row["PortfolioId"]
            print(f'Running calculation for portfolioID: {PortfolioId}')

            # Filter data for the current PortfolioId
            data = initial_data.filter(F.col('PortfolioId') == PortfolioId)
            
            IRR = irr_dict[PortfolioId]

            # Filter rows where Amount > 0
            WAL_calc = data.filter(F.col('Amount') > 0)

            # Calculate 'years' as (Date + 1 day - min(Date)) / 365
            min_date = WAL_calc.agg(F.min('Date')).collect()[0][0]
            WAL_calc = WAL_calc.withColumn('years', 
                        (F.datediff(WAL_calc['Date'], F.lit(min_date)) + 1) / 365)

            # Calculate 'product' (Amount * years)
            WAL_calc = WAL_calc.withColumn('product', F.col('Amount') * F.col('years'))

            # Group by 'key' and sum 'Amount' and 'product'
            WAL_calc_grouped = WAL_calc.groupBy('key') \
                                       .agg(F.sum('Amount').alias('Amount'),
                                            F.sum('product').alias('product'))

            # Calculate 'WAL' (product / Amount)
            WAL_calc_grouped = WAL_calc_grouped.withColumn('WAL', F.round(F.col('product') / F.col('Amount'), 3)) \
                                               .select('key', 'WAL')

            # NPV Calculation
            NPV_table = data.filter(F.col('Amount') > 0)

            # Calculate 'Date' difference in months
            date_diff = F.datediff(NPV_table['Date'], F.lit(min_date))
            years_diff = date_diff / 365
            months_diff = (date_diff % 365) / 30
            NPV_table = NPV_table.withColumn('Date', years_diff * 12 + months_diff)

            # Calculate NPV for each row
            NPV_table = NPV_table.withColumn('NPV', 
                            F.col('Amount') / (F.pow((1 + IRR / 12), F.col('Date'))))

            # Sum NPV per 'key'
            npv_grouped = NPV_table.groupBy('key').agg(F.sum('NPV').alias('NPV'))

            # Merge WAL and NPV results
            wal_npv = WAL_calc_grouped.join(npv_grouped, on='key', how='outer')

            # Ensure all debtors (keys) are represented
            debtors = data.select('key').distinct()
            final_table = debtors.join(wal_npv, on='key', how='left').fillna(0)

            # Store final table in the output dictionary
            output_table[PortfolioId] = final_table
            print('WAL,NPV calculation completed!')

        return output_table

    except Exception as e:
        print(f"Calculation failed! The following error occurred:\n{e}")
        return False
