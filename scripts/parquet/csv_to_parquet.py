from pyarrow import csv
import pyarrow.parquet as pq

csv_files = ['/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment_hasCreator_Person.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment_hasTag_Tag.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment_isLocatedIn_Place.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment_replyOf_Comment.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Comment_replyOf_Post.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Forum.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Forum_containerOf_Post.csv',
 '/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Forum_hasMember_Person.csv',
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Forum_hasModerator_Person.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Forum_hasTag_Tag.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Organisation.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Organisation_isLocatedIn_Place.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_hasInterest_Tag.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_isLocatedIn_Place.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_knows_Person.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_likes_Comment.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_likes_Post.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_studyAt_Organisation.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Person_workAt_Organisation.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Place.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Place_isPartOf_Place.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Post.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Post_hasCreator_Person.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Post_hasTag_Tag.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Post_isLocatedIn_Place.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Tag.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/TagClass.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/TagClass_isSubclassOf_TagClass.csv",
 "/Users/rfdavid/Devel/waterloo/kuzu/dataset/ldbc-sf01/Tag_hasType_TagClass.csv"]

read_options = csv.ReadOptions(autogenerate_column_names=False)
parse_options = csv.ParseOptions(delimiter="|")
for csv_file in csv_files:
    table = csv.read_csv(csv_file, read_options=read_options,
                         parse_options=parse_options)
    pq.write_table(table, csv_file.replace('.csv', '.parquet'))
