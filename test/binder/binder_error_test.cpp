#include "graph_test/graph_test.h"
#include "json.hpp"

using namespace kuzu::testing;

class BinderErrorTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }

    std::string getBindingError(const std::string& input) {
        auto result = conn->query(input);
        return result->getErrorMessage();
    }
};

TEST_F(BinderErrorTest, NodeTableNotExist) {
    std::string expectedException = "Binder exception: Node table PERSON does not exist.";
    auto input = "MATCH (a:PERSON) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RepeatedRelName) {
    std::string expectedException =
        "Binder exception: Bind relationship e1 to relationship with same name is not supported.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person)<-[e1:knows]-(:person) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RepeatedReturnColumnName) {
    std::string expectedException =
        "Binder exception: Multiple result column with the same name e1 are not supported.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN *, e1;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, WITHExpressionAliased) {
    std::string expectedException =
        "Binder exception: Expression in WITH must be aliased (use AS).";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH a.age RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindToDifferentVariableType1) {
    std::string expectedException = "Binder exception: e1 has data type REL. (NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH e1 AS a MATCH (a) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindToDifferentVariableType2) {
    std::string expectedException =
        "Binder exception: +(a.age,1) has data type INT64. (NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH a.age + 1 AS a MATCH (a) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindEmptyStar) {
    std::string expectedException =
        "Binder exception: RETURN or WITH * is not allowed when there are no variables in scope.";
    auto input = "RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindVariableNotInScope1) {
    std::string expectedException = "Binder exception: Variable a is not in scope.";
    auto input = "WITH a MATCH (a:person)-[e1:knows]->(b:person) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindVariableNotInScope2) {
    std::string expectedException = "Binder exception: Variable foo is not in scope.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WHERE a.age > foo RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindPropertyLookUpOnExpression) {
    std::string expectedException =
        "Binder exception: +(a.age,2) has data type INT64. (REL,NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN (a.age + 2).age;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindPropertyNotExist) {
    std::string expectedException = "Binder exception: Cannot find property foo for a.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN a.foo;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindPropertyNotExist2) {
    std::string expectedException = "Binder exception: Cannot find property foo for a.";
    auto input = "Create (a:person {foo:'x'});";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindIDArithmetic) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function "
        "+(INTERNAL_ID,INT64). Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT32,INT32) -> INT32\n(INT16,INT16) -> "
        "INT16\n(DOUBLE,DOUBLE) -> DOUBLE\n(FLOAT,FLOAT) -> "
        "FLOAT\n(INTERVAL,INTERVAL) -> INTERVAL\n(DATE,INT64) -> DATE\n(INT64,DATE) -> "
        "DATE\n(DATE,INTERVAL) -> DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> "
        "TIMESTAMP\n(INTERVAL,TIMESTAMP) -> TIMESTAMP\n";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WHERE id(a) + 1 < id(b) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindDateAddDate) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function "
        "+(DATE,DATE). Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT32,INT32) -> INT32\n(INT16,INT16) -> "
        "INT16\n(DOUBLE,DOUBLE) -> DOUBLE\n(FLOAT,FLOAT) -> FLOAT\n(INTERVAL,INTERVAL) -> "
        "INTERVAL\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n";
    auto input = "MATCH (a:person) RETURN a.birthdate + date('2031-02-01');";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindTimestampArithmetic) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function "
        "+(TIMESTAMP,INT64). Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT32,INT32) -> INT32\n(INT16,INT16) -> "
        "INT16\n(DOUBLE,DOUBLE) -> DOUBLE\n(FLOAT,FLOAT) -> FLOAT\n(INTERVAL,INTERVAL) -> "
        "INTERVAL\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n";
    auto input = "MATCH (a:person) WHERE a.registerTime + 1 < 5 RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindTimestampAddTimestamp) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function "
        "+(TIMESTAMP,TIMESTAMP). Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT32,INT32) -> INT32\n(INT16,INT16) -> "
        "INT16\n(DOUBLE,DOUBLE) -> DOUBLE\n(FLOAT,FLOAT) -> "
        "FLOAT\n(INTERVAL,INTERVAL) -> INTERVAL\n(DATE,INT64) -> DATE\n(INT64,DATE) -> "
        "DATE\n(DATE,INTERVAL) -> DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> "
        "TIMESTAMP\n(INTERVAL,TIMESTAMP) -> TIMESTAMP\n";
    auto input = "MATCH (a:person) RETURN a.registerTime + timestamp('2031-02-11 01:02:03');";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindNonExistingFunction) {
    std::string expectedException = "Catalog exception: DUMMY function does not exist.";
    auto input = "MATCH (a:person) WHERE dummy() < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindFunctionWithWrongNumParams) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function DATE. "
        "Supported inputs are\n(STRING) -> DATE\n";
    auto input = "MATCH (a:person) WHERE date() < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindFunctionWithWrongParamType) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function DATE(INT64). "
        "Supported inputs are\n(STRING) -> DATE\n";
    auto input = "MATCH (a:person) WHERE date(2012) < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OrderByVariableNotInScope) {
    std::string expectedException =
        "Binder exception: Order by expression a.ID is not in RETURN or WITH clause.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN SUM(a.age) ORDER BY a.ID;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, NestedAggregation) {
    std::string expectedException =
        "Binder exception: Expression SUM(SUM(a.age)) contains nested aggregation.";
    auto input = "MATCH (a:person) RETURN SUM(SUM(a.age));";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OptionalMatchAsFirstMatch) {
    std::string expectedException =
        "Binder exception: First match clause cannot be optional match.";
    auto input = "OPTIONAL MATCH (a:person) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OrderByWithoutSkipLimitInWithClause) {
    std::string expectedException =
        "Binder exception: In WITH clause, ORDER BY must be followed by SKIP or LIMIT.";
    auto input = "MATCH (a:person) WITH a.age AS k ORDER BY k RETURN k";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAllUnmatchedNumberOfExpressions) {
    std::string expectedException =
        "Binder exception: The number of columns to union/union all must be the same.";
    auto input = "MATCH (p:person) RETURN p.age,p.fName UNION ALL MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAllUnmatchedDataTypesOfExpressions) {
    std::string expectedException =
        "Binder exception: p1.age has data type INT64. (STRING) was expected.";
    auto input = "MATCH (p:person) RETURN p.fName UNION ALL MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAndUnionAllInSingleQuery) {
    std::string expectedException = "Binder exception: Union and union all can't be used together.";
    auto input = "MATCH (p:person) RETURN p.age UNION ALL MATCH (p1:person) RETURN p1.age UNION "
                 "MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, VarLenExtendZeroLowerBound) {
    std::string expectedException =
        "Binder exception: Lower and upper bound of a rel must be greater than 0.";
    auto input = "MATCH (a:person)-[:knows*0..5]->(b:person) return count(*)";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, SetDataTypeMisMatch) {
    std::string expectedException =
        "Binder exception: Expression hh has data type STRING but expect "
        "INT64. Implicit cast is not supported.";
    auto input = "MATCH (a:person) SET a.age = 'hh'";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateNodeDataTypeMisMatch) {
    std::string expectedException =
        "Binder exception: Expression hh has data type STRING but expect "
        "INT64. Implicit cast is not supported.";
    auto input = "CREATE (a:person {age:'hh'})";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelDataTypeMisMatch) {
    std::string expectedException =
        "Binder exception: Expression 12 has data type INT64 but expect "
        "DATE. Implicit cast is not supported.";
    auto input = "CREATE (a:person)-[:knows {date:12}]->(b:person)";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, ReadAfterUpdate1) {
    std::string expectedException = "Binder exception: Read after update is not supported.";
    auto input =
        "MATCH (a:person) SET a.age = 35 WITH a MATCH (a)-[:knows]->(b:person) RETURN a.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, ReadAfterUpdate3) {
    std::string expectedException = "Binder exception: Return/With after update is not supported.";
    auto input = "MATCH (a:person) SET a.age=3 RETURN a.fName";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DeleteNodeProperty) {
    std::string expectedException = "Binder exception: Delete PROPERTY is not supported.";
    auto input = "MATCH (a:person) DELETE a.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateNodeTableUsedName) {
    std::string expectedException = "Binder exception: Node person already exists.";
    auto input = "CREATE NODE TABLE person(NAME STRING, ID INT64, PRIMARY KEY(NAME))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateNodeTablePKColNameNotExists) {
    std::string expectedException =
        "Binder exception: Primary key dummyColName does not match any of "
        "the predefined node properties.";
    auto input = "CREATE NODE TABLE PERSON(NAME STRING, ID INT64, birthdate date, primary key "
                 "(dummyColName))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateNodeTableInvalidDataType) {
    std::string expectedException = "Cannot parse dataTypeID: BIGINT";
    auto input = "CREATE NODE TABLE PERSON(NAME BIGINT, PRIMARY KEY(NAME))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateNodeTableDuplicatedColumnName) {
    std::string expectedException =
        "Binder exception: Duplicated column name: eyesight, column name must be unique.";
    auto input =
        "CREATE NODE TABLE student (id INT64, eyesight double, eyesight double, PRIMARY KEY(id));";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CopyCSVInvalidParsingOption) {
    std::string expectedException = "Binder exception: Unrecognized parsing csv option: PK.";
    auto input = R"(COPY person FROM "person_0_0.csv" (pk=","))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CopyCSVInvalidSchemaName) {
    std::string expectedException = "Binder exception: Node/Rel university does not exist.";
    auto input = R"(COPY university FROM "person_0_0.csv" (pk=","))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CopyCSVInvalidEscapeChar) {
    std::string expectedException =
        "Binder exception: Copy csv option value can only be a single character with an "
        "optional escape character.";
    auto input = R"(COPY person FROM "person_0_0.csv" (ESCAPE=".."))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableUsedName) {
    std::string expectedException = "Binder exception: Rel knows already exists.";
    auto input = "CREATE REL TABLE knows ( FROM person TO person);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableInvalidNodeTableName) {
    std::string expectedException = "Binder exception: Node table post does not exist.";
    auto input = "CREATE REL TABLE knows_post ( FROM person TO post);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableInvalidRelMultiplicity) {
    std::string expectedException = "Catalog exception: Invalid relMultiplicity string 'MANY_LOT'.";
    auto input = "CREATE REL TABLE knows_post ( FROM person TO person, MANY_LOT);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableInvalidDataType) {
    std::string expectedException = "Cannot parse dataTypeID: SMALLINT";
    auto input = "CREATE REL TABLE knows_post ( FROM person TO person, ID SMALLINT, MANY_MANY);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableDuplicatedColumnName) {
    std::string expectedException =
        "Binder exception: Duplicated column name: date, column name must be unique.";
    auto input = "CREATE REL TABLE teaches (FROM person TO person, date DATE, date TIMESTAMP);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, CreateRelTableReservedColumnName) {
    std::string expectedException =
        "Binder exception: PropertyName: _id is an internal reserved propertyName.";
    auto input = "CREATE REL TABLE teaches (FROM person TO person, _id INT64);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DropNotExistsTable) {
    std::string expectedException = "Binder exception: Node/Rel person1 does not exist.";
    auto input = "DROP TABLE person1;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, SelfLoopRel) {
    std::string expectedException = "Binder exception: Self-loop rel e is not supported.";
    auto input = "MATCH (a:person)-[e:knows]->(a) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, InvalidLimitNumberType) {
    std::string expectedException =
        "Binder exception: The number of rows to skip/limit must be a non-negative integer.";
    auto input = "MATCH (a:person) RETURN a.age LIMIT \"abc\";";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, NegativeLimitNumber) {
    std::string expectedException =
        "Binder exception: The number of rows to skip/limit must be a non-negative integer.";
    auto input = "MATCH (a:person) RETURN a.age LIMIT -1;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DuplicateVariableName) {
    std::string expectedException = "Binder exception: Variable a already exists.";
    auto input = "MATCH (a:person) UNWIND [1,2] AS a RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, MaxNodeID) {
    std::string expectedException =
        "Binder exception: Cannot match a built-in function for given function MIN(INTERNAL_ID). "
        "Supported inputs are\nDISTINCT (BOOL) -> BOOL\n(BOOL) -> BOOL\nDISTINCT (INT64) -> "
        "INT64\n(INT64) -> INT64\nDISTINCT (INT32) -> INT32\n(INT32) -> INT32\nDISTINCT (INT16) -> "
        "INT16\n(INT16) -> INT16\nDISTINCT (DOUBLE) -> DOUBLE\n(DOUBLE) -> DOUBLE\nDISTINCT "
        "(FLOAT) -> FLOAT\n(FLOAT) -> FLOAT\nDISTINCT "
        "(DATE) -> DATE\n(DATE) -> DATE\nDISTINCT (TIMESTAMP) -> TIMESTAMP\n(TIMESTAMP) -> "
        "TIMESTAMP\nDISTINCT (INTERVAL) -> INTERVAL\n(INTERVAL) -> INTERVAL\nDISTINCT (STRING) -> "
        "STRING\n(STRING) -> STRING\n";
    auto input = "MATCH (a:person) RETURN MIN(a);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OrderByNodeID) {
    std::string expectedException =
        "Binder exception: Cannot order by p. Order by node or rel is not supported.";
    auto input = "match (p:person) with p as x order by x skip 1 return x;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DropColumnFromNonExistedTable) {
    std::string expectedException = "Binder exception: Node/Rel person1 does not exist.";
    auto input = "alter table person1 drop k";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DropNonExistedColumn) {
    std::string expectedException = "Binder exception: person table doesn't have property: random.";
    auto input = "alter table person drop random";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DropPrimaryKeyColumn) {
    std::string expectedException = "Binder exception: Cannot drop primary key of a node table.";
    auto input = "alter table person drop ID";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, AddPropertyDuplicateName) {
    std::string expectedException = "Binder exception: Property: fName already exists.";
    auto input = "alter table person add fName STRING";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, AddPropertyUnmatchedDefaultValueType) {
    std::string expectedException = "Binder exception: Expression 3.2 has data type STRING but "
                                    "expect INT64. Implicit cast is not supported.";
    auto input = "alter table person add intCol INT64 DEFAULT '3.2'";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RenameNonExistedTable) {
    std::string expectedException = "Binder exception: Node/Rel person1 does not exist.";
    auto input = "alter table person1 rename to person2";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RenameTableDuplicateName) {
    std::string expectedException = "Binder exception: Table: organisation already exists.";
    auto input = "alter table person rename to organisation";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RenamePropertyOfNonExistedTable) {
    std::string expectedException = "Binder exception: Node/Rel person1 does not exist.";
    auto input = "alter table person1 rename col1 to col2";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RenamePropertyDuplicateName) {
    std::string expectedException =
        "Binder exception: Property gender already exists in table: person.";
    auto input = "alter table person rename fName to gender";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, EmptyList) {
    std::string expectedException =
        "Binder exception: Cannot resolve child data type for LIST_CREATION.";
    auto input = "RETURN []";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, InvalidFixedListChildType) {
    std::string expectedException =
        "Binder exception: The child type of a fixed list must be a numeric type. Given: STRING.";
    auto input = "create node table test1(ID INT64, marks STRING[5], PRIMARY KEY(ID))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, InvalidFixedListNumElements) {
    std::string expectedException = "Binder exception: The number of elements in a fixed list must "
                                    "be greater than 0. Given: 0.";
    auto input = "create node table test1(ID INT64, marks INT64[0], PRIMARY KEY(ID))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, InvalidFixedListSize) {
    std::string expectedException =
        "Binder exception: Cannot store a fixed list of size 4096 in a page.";
    auto input = "create node table test1(ID INT64, marks INT64[512], PRIMARY KEY(ID))";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}
