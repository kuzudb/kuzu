create node table City (id int64, PRIMARY KEY (id));
create node table Comment (id int64, PRIMARY KEY (id));
create node table Company (id int64, PRIMARY KEY (id));
create node table Continent (id int64, PRIMARY KEY (id));
create node table Country (id int64, PRIMARY KEY (id));
create node table Forum (id int64, PRIMARY KEY (id));
create node table Person (id int64, PRIMARY KEY (id));
create node table Post (id int64, PRIMARY KEY (id));
create node table Tag (id int64, PRIMARY KEY (id));
create node table TagClass (id int64, PRIMARY KEY (id));
create node table University (id int64, PRIMARY KEY (id));
create rel table City_isPartOf_Country (FROM City TO Country, MANY_ONE);
create rel table Comment_hasCreator_Person (FROM Comment TO Person, MANY_ONE);
create rel table Comment_hasTag_Tag (FROM Comment TO Tag);
create rel table Comment_isLocatedIn_Country (FROM Comment TO Country, MANY_ONE);
create rel table Comment_replyOf_Comment (FROM Comment TO Comment, MANY_ONE);
create rel table Comment_replyOf_Post (FROM Comment TO Post, MANY_ONE);
create rel table Company_isLocatedIn_Country (FROM Company TO Country, MANY_ONE);
create rel table Country_isPartOf_Continent (FROM Country TO Continent, MANY_ONE);
create rel table Forum_containerOf_Post (FROM Forum TO Post);
create rel table Forum_hasMember_Person (FROM Forum TO Person);
create rel table Forum_hasModerator_Person (FROM Forum TO Person);
create rel table Forum_hasTag_Tag (FROM Forum TO Tag);
create rel table Person_hasInterest_Tag (FROM Person TO Tag);
create rel table Person_isLocatedIn_City (FROM Person TO City, MANY_ONE);
create rel table Person_knows_Person (FROM Person TO Person);
create rel table Person_likes_Comment (FROM Person TO Comment);
create rel table Person_likes_Post (FROM Person TO Post);
create rel table Person_studyAt_University (FROM Person TO University, MANY_ONE);
create rel table Person_workAt_Company (FROM Person TO Company);
create rel table Post_hasCreator_Person (FROM Post TO Person, MANY_ONE);
create rel table Post_hasTag_Tag (FROM Post TO Tag);
create rel table Post_isLocatedIn_Country (FROM Post TO Country, MANY_ONE);
create rel table Tag_hasType_TagClass (FROM Tag to TagClass, MANY_ONE);
create rel table TagClass_isSubclassOf_TagClass (FROM TagClass to TagClass, MANY_ONE);
create rel table University_isLocatedIn_City (FROM University TO City, MANY_ONE);
