import os
import logging
import shutil
import sys

base_dir = os.path.dirname(os.path.realpath(__file__))

# import kuzu Python API
sys.path.append(os.path.join(base_dir, '..', '..'))
import tools.python_api.build.kuzu as kuzu

def _get_kuzu_version():
    cmake_file = os.path.join(base_dir, '..', '..', 'CMakeLists.txt')
    with open(cmake_file) as f:
        for line in f:
            if line.startswith('project(Kuzu VERSION'):
                return line.split(' ')[2].strip()
            
            
def check_updated_version(bin_version, dataset_name, serialized_graph_path):
    if not os.path.exists(serialized_graph_path):
        os.mkdir(serialized_graph_path)

    if os.path.exists(os.path.join(serialized_graph_path, 'version.txt')):
        with open(os.path.join(serialized_graph_path, 'version.txt')) as f:
            dataset_version = f.readline().strip()
            if dataset_version == bin_version:
                logging.info(
                    'Dataset %s has version of %s, which matches the database version, skip serializing', dataset_name,
                    bin_version)
                return True
            else:
                logging.info(
                    'Dataset %s has version of %s, which does not match the database version %s, serializing dataset...',
                    dataset_name, dataset_version, bin_version)
    else:
        logging.info('Dataset %s does not exist or does not have a version file, serializing dataset...', dataset_name)

    return False

            
def convert(dataset_path):
    os.chdir(dataset_path)

    converted_flag = os.path.join(dataset_path, '.converted')
    if not os.path.exists(converted_flag):
        logging.info("Creating CSV file Person_knows_Person_bidirectional.csv")
        with open('Person_knows_Person.csv', 'r') as person_knows_person, \
            open('Person_knows_Person_bidirectional.csv', 'w') as bidirectional_file:
            bidirectional_file.write(person_knows_person.read())
            person_knows_person.seek(0)
            bidirectional_file.writelines('|'.join(line.strip().split('|')[1::-1]) + '\n' for line in person_knows_person.readlines()[1:])

        # Define the conversion candidates
        CONVERSION_CANDIDATES = [
            "Comment Post Message",
            "Comment_hasCreator_Person Post_hasCreator_Person Message_hasCreator_Person",
            "Comment_hasTag_Tag Post_hasTag_Tag Message_hasTag_Tag",
            "Comment_isLocatedIn_Country Post_isLocatedIn_Country Message_isLocatedIn_Country",
            "Comment_replyOf_Comment Comment_replyOf_Post Message_replyOf_Message",
            "Person_likes_Comment Person_likes_Post Person_likes_Message"
        ]

        for CONVERSION_CANDIDATE in CONVERSION_CANDIDATES:
            CAND = CONVERSION_CANDIDATE.split()
            logging.info(f"Creating CSV file {CAND[2]}.csv")

            source_path1 = f'{CAND[0]}.csv'
            source_path2 = f'{CAND[1]}.csv'
            target_path = f'{CAND[2]}.csv'

            # Read the content of both source files
            with open(source_path1, 'r') as source_file1, open(source_path2, 'r') as source_file2:
                content1 = source_file1.read()
                # Skip the header of the second file
                content2 = ''.join(source_file2.readlines()[1:])

            # Concatenate the content and write to the target file
            with open(target_path, 'w') as target_file:
                target_file.write(content1 + content2)


        # Forum_containerOf_Message always points to Posts
        shutil.copy('Forum_containerOf_Post.csv', 'Forum_containerOf_Message.csv')

        # Create the .converted flag file
        open(converted_flag, 'a').close()


def load_schema(conn):
    schema_file = open(os.path.join(base_dir, 'schema.cypher')).read().split(';\n')

    for schema in schema_file:
        if schema == "":
            continue
        logging.info(f"Loading schema {schema}")
        results = conn.execute(f"{schema}")
        if results.has_next():
            logging.info(results.get_next()[0])
            logging.info(f"Compiling time (s): {results.get_compiling_time() / 1000:.4f}")
            logging.info(f"Execution time (s): {results.get_execution_time() / 1000:.4f}\n")

    logging.info("Loaded schema")


def load_lsqb_dataset(conn, data_path):
    lsqb_node_files = ["Company", "University", "Continent", "Country", "City", "Tag", "TagClass", "Forum", "Comment", "Post", "Person"]
    lsqb_edge_files = ["City_isPartOf_Country", "Comment_hasCreator_Person", "Comment_hasTag_Tag", "Comment_isLocatedIn_Country", "Comment_replyOf_Comment", "Comment_replyOf_Post", "Company_isLocatedIn_Country", "Country_isPartOf_Continent", "Forum_containerOf_Post", "Forum_hasMember_Person", "Forum_hasModerator_Person", "Forum_hasTag_Tag", "Person_hasInterest_Tag", "Person_isLocatedIn_City", "Person_knows_Person_bidirectional", "Person_likes_Comment", "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post_hasCreator_Person", "Post_hasTag_Tag", "Post_isLocatedIn_Country", "TagClass_isSubclassOf_TagClass", "Tag_hasType_TagClass", "University_isLocatedIn_City"]

    load_schema(conn)

    extension = ".csv"
    copy_options = "(HEADER=True, DELIM='|')"

    for lsqb_file in lsqb_node_files:
        logging.info(f"Loading {lsqb_file}")
        results = conn.execute(f"""COPY {lsqb_file} from '{data_path}/{lsqb_file}{extension}' {copy_options}""")
        if results.has_next():
            logging.info(results.get_next()[0])
            logging.info(f"Compiling time (s): {results.get_compiling_time() / 1000:.4f}")
            logging.info(f"Execution time (s): {results.get_execution_time() / 1000:.4f}\n")

    for lsqb_file in lsqb_edge_files:
        logging.info(f"Loading {lsqb_file}")
        if "Person_knows_Person_bidirectional" in lsqb_file:
            results = conn.execute(f"""COPY Person_knows_Person from '{data_path}/{lsqb_file}{extension}' {copy_options}""")
        else:
            results = conn.execute(f"""COPY {lsqb_file} from '{data_path}/{lsqb_file}{extension}' {copy_options}""")
        if results.has_next():
            logging.info(results.get_next()[0])
            logging.info(f"Compiling time (s): {results.get_compiling_time() / 1000:.4f}")
            logging.info(f"Execution time (s): {results.get_execution_time() / 1000:.4f}\n")

    logging.info("Loaded data files")


def serialize(dataset_name, dataset_path, serialized_graph_path):
    convert(dataset_path)

    bin_version = _get_kuzu_version()
    if check_updated_version(bin_version, dataset_name, serialized_graph_path):
        return

    shutil.rmtree(serialized_graph_path, ignore_errors=True)
    os.mkdir(serialized_graph_path)

    db = kuzu.Database(serialized_graph_path)
    conn = kuzu.Connection(db)
    logging.info("Successfully connected")

    load_lsqb_dataset(conn, dataset_path)

    with open(os.path.join(serialized_graph_path, 'version.txt'), 'w') as f:
        f.write(bin_version)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dataset_name = sys.argv[1]
    dataset_path = sys.argv[2]
    serialized_graph_path = sys.argv[3]
    try:
        serialize(dataset_name, dataset_path, serialized_graph_path)
    except Exception as e:
        logging.error('Error serializing dataset %s', dataset_name)
        logging.error(e)
        sys.exit(1)
    finally:
        shutil.rmtree(os.path.join(base_dir, 'history.txt'),
                      ignore_errors=True)
