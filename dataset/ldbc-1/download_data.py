import os
import urllib.request
import sys
from urllib.error import URLError

FILE_PATH = __file__
BASE_URL = "https://raw.githubusercontent.com/kuzudb/kuzu-swift-demo/main/Datasets/ldbc-1/csv"

CSV_FILES = [
    "comment_0_0.csv",
    "comment_0_1.csv", 
    "comment_0_2.csv",
    "comment_hasCreator_person_0_0.csv",
    "comment_hasTag_tag_0_0.csv",
    "comment_isLocatedIn_place_0_0.csv",
    "comment_replyOf_comment_0_0.csv",
    "comment_replyOf_post_0_0.csv",
    "forum_0_0.csv",
    "forum_containerOf_post_0_0.csv",
    "forum_hasMember_person_0_0.csv",
    "forum_hasModerator_person_0_0.csv",
    "forum_hasTag_tag_0_0.csv",
    "organisation_0_0.csv",
    "organisation_isLocatedIn_place_0_0.csv",
    "person_0_0.csv",
    "person_email_emailaddress_0_0.csv",
    "person_hasInterest_tag_0_0.csv",
    "person_isLocatedIn_place_0_0.csv",
    "person_knows_person_0_0.csv",
    "person_likes_comment_0_0.csv",
    "person_likes_post_0_0.csv",
    "person_speaks_language_0_0.csv",
    "person_studyAt_organisation_0_0.csv",
    "person_workAt_organisation_0_0.csv",
    "place_0_0.csv",
    "place_isPartOf_place_0_0.csv",
    "post_0_0.csv",
    "post_0_1.csv",
    "post_hasCreator_person_0_0.csv",
    "post_hasTag_tag_0_0.csv",
    "post_isLocatedIn_place_0_0.csv",
    "tag_0_0.csv",
    "tag_hasType_tagclass_0_0.csv",
    "tagclass_0_0.csv",
    "tagclass_isSubclassOf_tagclass_0_0.csv"
]

BASE_PATH = os.path.dirname(FILE_PATH)
CSV_PATH = os.path.join(BASE_PATH, "csv")

if os.path.exists(CSV_PATH):
    print("CSV directory already exists. Skipping download.")
    sys.exit(0)

print("Creating CSV directory at '%s'..." % CSV_PATH)
os.makedirs(CSV_PATH, exist_ok=True)

print("Downloading %d CSV files from GitHub repository..." % len(CSV_FILES))

failed_downloads = []
for i, csv_file in enumerate(CSV_FILES, 1):
    file_url = "%s/%s" % (BASE_URL, csv_file)
    local_path = os.path.join(CSV_PATH, csv_file)
    
    try:
        print("[%d/%d] Downloading %s..." % (i, len(CSV_FILES), csv_file))
        urllib.request.urlretrieve(file_url, local_path)
    except URLError as e:
        print("Failed to download %s: %s" % (csv_file, e))
        failed_downloads.append(csv_file)
        continue

if failed_downloads:
    print("Warning: %d files failed to download:" % len(failed_downloads))
    for failed_file in failed_downloads:
        print("  - %s" % failed_file)
    sys.exit(1)

print("All done.")
