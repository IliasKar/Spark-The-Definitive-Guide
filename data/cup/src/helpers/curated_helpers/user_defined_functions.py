from typing import Optional

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def get_bucket_from_dpd(dpd: int) -> Optional[int]:
    if dpd is None:
        return None
    elif dpd <= 0:
        return 0
    elif 1 <= dpd <= 29:
        return 1
    elif 30 <= dpd <= 59:
        return 2
    elif 60 <= dpd <= 89:
        return 3
    elif 90 <= dpd <= 119:
        return 4
    elif 120 <= dpd <= 149:
        return 5
    elif 150 <= dpd <= 179:
        return 6
    elif 180 <= dpd <= 209:
        return 7
    elif 210 <= dpd <= 239:
        return 8
    elif 240 <= dpd <= 269:
        return 9
    elif 270 <= dpd <= 299:
        return 10
    elif 300 <= dpd <= 329:
        return 11
    elif 330 <= dpd <= 359:
        return 12
    elif 360 <= dpd <= 720:
        return 13
    elif dpd > 720:
        return 14
    else:
        return None


# Convert your Python function into a UDF
get_bucket_from_dpd_udf = udf(get_bucket_from_dpd, IntegerType())
