# Databricks notebook source
# MAGIC %md
# MAGIC # RunDeployment — dispatcher για το ημερήσιο deployment notebook
# MAGIC
# MAGIC Σταθερό notebook. Το task στο Release Job δείχνει **πάντα** εδώ και δεν
# MAGIC αλλάζει ποτέ. Κάθε φορά που κάνεις `Run now`, ο dispatcher βρίσκει μόνος
# MAGIC του τη σημερινή ημερομηνία (ώρα Αθήνας) και τρέχει το αντίστοιχο αρχείο
# MAGIC μέσα στον φάκελο `Deployments/`, π.χ. `Deployments/2026-09-29`.
# MAGIC
# MAGIC Δεν χρειάζεται να περνάς ημερομηνία σαν parameter. Το μόνο που κάνεις
# MAGIC κάθε Τρίτη/Πέμπτη είναι:
# MAGIC 1. Φτιάχνεις το νέο `Deployments/<YYYY-MM-DD>` notebook (`.py` ή `.ipynb`)
# MAGIC 2. commit + push
# MAGIC 3. `Run now`
# MAGIC
# MAGIC **Προαιρετικό override:** αν χρειαστεί να τρέξεις χειροκίνητα άλλη
# MAGIC ημερομηνία, γέμισε το widget `override_date` με μορφή `YYYY-MM-DD`.
# MAGIC Στην κανονική ροή το αφήνεις κενό.

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

# Ρυθμίσεις
TIMEZONE = "Europe/Athens"               # ώστε serverless/UTC clusters να μη γυρίζουν λάθος μέρα
DATE_FMT = "%Y-%m-%d"                     # μορφή ονόματος αρχείου, π.χ. 2026-09-29

# COMMAND ----------

# MAGIC %md ## 1. Βρες την ημερομηνία (override ή σημερινή σε ώρα Αθήνας)

# COMMAND ----------

dbutils.widgets.text("override_date", "", "Override date (YYYY-MM-DD) — κενό = σημερινή")
override_date = dbutils.widgets.get("override_date").strip()

if override_date:
    # validate ότι είναι σωστή ημερομηνία στη σωστή μορφή
    try:
        datetime.strptime(override_date, DATE_FMT)
    except ValueError:
        raise ValueError(
            f"override_date='{override_date}' δεν είναι έγκυρη ημερομηνία. "
            f"Περιμένω μορφή {DATE_FMT}, π.χ. 2026-09-29."
        )
    deployment_date = override_date
    date_source = "override_date widget"
else:
    deployment_date = datetime.now(ZoneInfo(TIMEZONE)).strftime(DATE_FMT)
    date_source = f"σημερινή ({TIMEZONE})"

print(f"Deployment date : {deployment_date}")
print(f"Πηγή ημερομηνίας : {date_source}")

# COMMAND ----------

# MAGIC %md ## 2. Τρέξε το ημερήσιο deployment notebook
# MAGIC
# MAGIC Δίνουμε στο `dbutils.notebook.run()` **σκέτο relative path** (το όνομα της
# MAGIC ημερομηνίας, χωρίς extension). Το Databricks το κάνει resolve σε σχέση με
# MAGIC τον φάκελο του **τρέχοντος notebook όπως εκτελείται** — δηλαδή το Git
# MAGIC checkout του job (το repo/branch που έχει οριστεί στο task), **όχι** το
# MAGIC workspace checkout του χρήστη.
# MAGIC
# MAGIC Γι' αυτό **δεν** χρησιμοποιούμε `notebookPath()` ούτε έλεγχο ύπαρξης μέσω
# MAGIC `os.path`/Workspace API: αυτά δείχνουν στο workspace του χρήστη και
# MAGIC αποτυγχάνουν άδικα όταν τρέχεις από άλλο branch ή δεν έχεις το ημερήσιο
# MAGIC notebook τοπικά. Αν το αρχείο λείπει στο branch του job, το
# MAGIC `dbutils.notebook.run` πετάει καθαρό σφάλμα — το τυλίγουμε για φιλικό μήνυμα.

# COMMAND ----------

# relative ως προς τον φάκελο του RunDeployment (Deployments/), χωρίς extension.
# Το Databricks βρίσκει το notebook είτε είναι .py είτε .ipynb.
target_notebook = f"{deployment_date}"

print(f"Deployment date : {deployment_date}")
print(f"Target notebook : {target_notebook}  (relative ως προς Deployments/ στο git checkout του job)")

# COMMAND ----------

print(f"Εκτέλεση: {target_notebook}")
try:
    result = dbutils.notebook.run(target_notebook, timeout_seconds=0)
except Exception as e:
    raise RuntimeError(
        f"Απέτυχε η εκτέλεση του deployment notebook 'Deployments/{deployment_date}'.\n"
        f"Πιθανή αιτία: δεν υπάρχει το αρχείο Deployments/{deployment_date} (.py ή .ipynb) "
        f"στο branch που τρέχει το job.\n"
        f"Βεβαιώσου ότι:\n"
        f"  • έχεις φτιάξει το αρχείο Deployments/{deployment_date}\n"
        f"  • έγινε commit + push στο branch που δείχνει το job (Git source)\n"
        f"  • το όνομα ταιριάζει ακριβώς με τη μορφή {DATE_FMT}\n"
        f"  • για χειροκίνητη εκτέλεση άλλης ημερομηνίας, γέμισε το widget 'override_date'.\n"
        f"Αρχικό σφάλμα: {e}"
    ) from e

print("Ολοκληρώθηκε.")
print(f"Επιστροφή: {result}")
