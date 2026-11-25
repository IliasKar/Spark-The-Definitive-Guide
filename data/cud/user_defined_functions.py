from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
import unicodedata
import re 

def get_bucket_from_dpd( dpd : int ):
        
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


# Convert your Python function into a UDF
get_bucket_from_dpd_udf = udf(get_bucket_from_dpd, IntegerType())



def is_valid_tax_number(Input : str):
    if Input is None:
        return None
    elif not Input.isdigit():
        return None
    elif Input == '999999993':
        return None
    elif any(c in Input for c in ['+', '-', '.', ',', 'E', 'e']):
        return None 
    elif len(Input) == 8:
        Input = '0' + Input
    if len(Input) != 9:
        return None
    factor = 256
    check_digit = int(Input[8])
    value =0
    for i in range(0,8):
        digit = int(Input[i])
        value = value + digit * factor
        factor = factor // 2
    if value == 0:
        return False
    checksum = value % 11
    if checksum == 10:
        checksum=0
    if checksum == check_digit:
        return True
    else:
        return False

# Convert your Python function into a UDF
is_valid_tax_number_udf = udf(is_valid_tax_number, BooleanType())



def strip_greek_accents(input_string):
   # Normalize the string to decompose composite characters
   normalized = unicodedata.normalize("NFD", input_string)

   # Use a filter to remove diacritic marks and join characters back together
   stripped_string = "".join(
       [char for char in normalized if unicodedata.category(char) != "Mn"]
   )
   return stripped_string



# Text Normalization

def remove_substrings(s: str, 
                      to_remove: list = ["nan", "none", "null", "\n"]) -> str:

    for substring in to_remove:
        s = s.replace(substring, "")

    return s

def remove_spaces(s: str) -> str:
    return re.sub(r"\s+", "", s)

# My addition - This function removes all punctuation from s
def remove_punctuation(s: str) -> str:
    return re.sub(r'[^\w\s]', '', s)

def replace_final_sigma(s):
    return s.replace('ς', 'σ')

def string_cast_normalize(s: int | float | str) -> str:

    s = str(s)
    s = s.lower()
    s = strip_greek_accents(s)
    s = remove_spaces(s)
    s = remove_substrings(s)
    s = remove_punctuation(s)
    s = replace_final_sigma(s)

    return s


########################################################
#---------------levenshtein_similarity----------------#
########################################################

def levenshtein_similarity(s1, s2):
    """
    Compute the Levenshtein similarity between two strings. 
    The similarity is a relative measure which is 1 - (edit_distance(s1, s2) / max(len(s1), len(s2))).
    """
    if len(s1) < len(s2):
        return levenshtein_similarity(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    distance = previous_row[-1]
    max_len = max(len(s1), len(s2))
    return 1 - distance / max_len  # Convert distance to similarity

#################################################################
# Convert your Python function into a UDF
string_cast_normalize_udf = udf(string_cast_normalize, StringType())

levenshtein_udf=udf(levenshtein_similarity,FloatType())
