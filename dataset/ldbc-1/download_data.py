import os
import urllib.request
import sys
import subprocess
import ssl
from urllib.error import URLError

FILE_PATH = __file__
URL = "https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/social_network-sf1-CsvBasic-StringDateFormatter.tar.zst"

BASE_PATH = os.path.dirname(FILE_PATH)
ARCHIVE_PATH = os.path.join(BASE_PATH, "social_network-sf1-CsvBasic-StringDateFormatter.tar.zst")
UNARCHIVED_PATH = os.path.join(BASE_PATH, "social_network-sf1-CsvBasic-StringDateFormatter")
CSV_PATH = os.path.join(BASE_PATH, "csv")

if os.path.exists(CSV_PATH):
    print("CSV directory already exists. Skipping download.")
    sys.exit(0)

print("Downloading LDBC SF1 dataset from '%s'..." % URL)
print("This may take a while as the file is large...")

# Create SSL context that doesn't verify certificates (for compatibility)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

try:
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl_context))
    urllib.request.install_opener(opener)
    urllib.request.urlretrieve(URL, ARCHIVE_PATH)
    print("Download completed.")
except URLError as e:
    print("Failed to download file: %s" % e)
    sys.exit(1)

print("Extracting tar.zst archive...")
try:
    # Check if zstd is available
    subprocess.check_call(["zstd", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # Extract using zstd and tar
    subprocess.check_call(["zstd", "-d", ARCHIVE_PATH, "-c"], stdout=open(ARCHIVE_PATH[:-4], 'w'))
    subprocess.check_call(["tar", "-xf", ARCHIVE_PATH[:-4], "-C", BASE_PATH])
    os.remove(ARCHIVE_PATH[:-4])  # Remove the uncompressed tar file
except (subprocess.CalledProcessError, OSError):
    print("zstd not found. Trying with tar directly (requires GNU tar with zstd support)...")
    try:
        subprocess.check_call(["tar", "--zstd", "-xf", ARCHIVE_PATH, "-C", BASE_PATH])
    except subprocess.CalledProcessError:
        print("Error: Cannot extract tar.zst file. Please install zstd or use GNU tar with zstd support.")
        print("On macOS: brew install zstd")
        print("On Ubuntu/Debian: sudo apt-get install zstd")
        os.remove(ARCHIVE_PATH)
        sys.exit(1)

print("Removing archive file...")
os.remove(ARCHIVE_PATH)

# Rename the extracted directory to a more manageable name CSV_PATH
if os.path.exists(UNARCHIVED_PATH):
    print("Renaming extracted directory to 'csv'...")
    os.rename(UNARCHIVED_PATH, CSV_PATH)
    print("All done.")
else:
    print("Error: Extracted directory does not exist. Please check the extraction process.")
    sys.exit(1)

