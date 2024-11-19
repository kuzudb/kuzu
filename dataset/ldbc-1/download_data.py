URL = "http://rgw.cs.uwaterloo.ca/kuzu-test/ldbc-1-csv/csv.zip"
FILE_PATH = __file__

import os
import urllib.request
import sys
import zipfile

BASE_PATH = os.path.dirname(FILE_PATH)
CSV_ZIP_PATH = os.path.join(BASE_PATH, "csv.zip")
CSV_PATH = os.path.join(BASE_PATH, "csv")
if os.path.exists(CSV_PATH):
    sys.exit(0)

print("Downloading CSV files from '%s' to '%s'..." % (URL, CSV_ZIP_PATH))
urllib.request.urlretrieve(URL, CSV_ZIP_PATH)


print("Extracting CSV files to '%s'..." % BASE_PATH)
with zipfile.ZipFile(CSV_ZIP_PATH, "r") as zip_ref:
    zip_ref.extractall(BASE_PATH)

print("Removing CSV zip file '%s'..." % CSV_ZIP_PATH)
os.remove(CSV_ZIP_PATH)

print("All done.")
