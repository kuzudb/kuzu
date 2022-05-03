#include "gtest/gtest.h"
#include "test/mock/mock_catalog.h"

#include "src/binder/include/query_binder.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;
using namespace graphflow::binder;

using ::testing::NiceMock;
using ::testing::Test;

class BinderErrorTest : public Test {

public:
    void SetUp() override { catalog.setUp(); }

    string getBindingError(const string& input) {
        try {
            auto parsedQuery = Parser::parseQuery(input);
            QueryBinder(catalog).bind(*parsedQuery);
        } catch (const Exception& exception) { return exception.what(); }
        return string();
    }

private:
    NiceMock<TinySnbCatalog> catalog;
};

TEST_F(BinderErrorTest, DisconnectedGraph1) {
    string expectedException = "Binder exception: Disconnect query graph is not supported.";
    auto input = "MATCH (a:person), (b:person) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, DisconnectedGraph2) {
    string expectedException = "Binder exception: Disconnect query graph is not supported.";
    auto input = "MATCH (a:person) WITH * MATCH (b:person) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, NodeLabelNotExist) {
    string expectedException = "Binder exception: Node label PERSON does not exist.";
    auto input = "MATCH (a:PERSON) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, NodeRelNotConnect) {
    string expectedException =
        "Binder exception: Node label person doesn't connect to rel label workAt.";
    auto input = "MATCH (a:person)-[e1:workAt]->(b:person) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RepeatedRelName) {
    string expectedException =
        "Binder exception: Bind relationship e1 to relationship with same name is not supported.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person)<-[e1:knows]-(:person) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, RepeatedReturnColumnName) {
    string expectedException =
        "Binder exception: Multiple result column with the same name e1 are not supported.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN *, e1;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, WITHExpressionAliased) {
    string expectedException = "Binder exception: Expression in WITH must be aliased (use AS).";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH a.age RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindToDifferentVariableType1) {
    string expectedException = "Binder exception: e1 has data type REL. (NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH e1 AS a MATCH (a) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindToDifferentVariableType2) {
    string expectedException =
        "Binder exception: a.age + 1 has data type INT64. (NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WITH a.age + 1 AS a MATCH (a) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindEmptyStar) {
    string expectedException =
        "Binder exception: RETURN or WITH * is not allowed when there are no variables in scope.";
    auto input = "RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindVariableNotInScope1) {
    string expectedException = "Binder exception: Variable a is not in scope.";
    auto input = "WITH a MATCH (a:person)-[e1:knows]->(b:person) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindVariableNotInScope2) {
    string expectedException = "Binder exception: Variable foo is not in scope.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WHERE a.age > foo RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindPropertyLookUpOnExpression) {
    string expectedException =
        "Binder exception: a.age + 2 has data type INT64. (REL,NODE) was expected.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN (a.age + 2).age;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindPropertyNotExist) {
    string expectedException = "Binder exception: Node a does not have property foo.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN a.foo;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindIDArithmetic) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function +(NODE_ID,INT64). "
        "Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT64,DOUBLE) -> DOUBLE\n(DOUBLE,INT64) -> "
        "DOUBLE\n(DOUBLE,DOUBLE) -> DOUBLE\n(UNSTRUCTURED,UNSTRUCTURED) -> "
        "UNSTRUCTURED\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n(INTERVAL,INTERVAL) -> INTERVAL\n";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) WHERE id(a) + 1 < id(b) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindDateAddDate) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function +(DATE,DATE). "
        "Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT64,DOUBLE) -> DOUBLE\n(DOUBLE,INT64) -> "
        "DOUBLE\n(DOUBLE,DOUBLE) -> DOUBLE\n(UNSTRUCTURED,UNSTRUCTURED) -> "
        "UNSTRUCTURED\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n(INTERVAL,INTERVAL) -> INTERVAL\n";
    auto input = "MATCH (a:person) RETURN a.birthdate + date('2031-02-01');";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindTimestampArithmetic) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function +(TIMESTAMP,INT64). "
        "Supported inputs "
        "are\n(INT64,INT64) -> INT64\n(INT64,DOUBLE) -> DOUBLE\n(DOUBLE,INT64) -> "
        "DOUBLE\n(DOUBLE,DOUBLE) -> DOUBLE\n(UNSTRUCTURED,UNSTRUCTURED) -> "
        "UNSTRUCTURED\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n(INTERVAL,INTERVAL) -> INTERVAL\n";
    auto input = "MATCH (a:person) WHERE a.registerTime + 1 < 5 RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindTimestampAddTimestamp) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function "
        "+(TIMESTAMP,TIMESTAMP). Supported "
        "inputs are\n(INT64,INT64) -> INT64\n(INT64,DOUBLE) -> DOUBLE\n(DOUBLE,INT64) -> "
        "DOUBLE\n(DOUBLE,DOUBLE) -> DOUBLE\n(UNSTRUCTURED,UNSTRUCTURED) -> "
        "UNSTRUCTURED\n(DATE,INT64) -> DATE\n(INT64,DATE) -> DATE\n(DATE,INTERVAL) -> "
        "DATE\n(INTERVAL,DATE) -> DATE\n(TIMESTAMP,INTERVAL) -> TIMESTAMP\n(INTERVAL,TIMESTAMP) -> "
        "TIMESTAMP\n(INTERVAL,INTERVAL) -> INTERVAL\n";
    auto input = "MATCH (a:person) RETURN a.registerTime + timestamp('2031-02-11 01:02:03');";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindNonExistingFunction) {
    string expectedException = "Catalog exception: DUMMY function does not exist.";
    auto input = "MATCH (a:person) WHERE dummy() < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindFunctionWithWrongNumParams) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function DATE. "
        "Supported inputs are\n(STRING) -> DATE\n";
    auto input = "MATCH (a:person) WHERE date() < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, BindFunctionWithWrongParamType) {
    string expectedException =
        "Binder exception: Cannot match a built-in function for given function DATE(INT64). "
        "Supported inputs are\n(STRING) -> DATE\n";
    auto input = "MATCH (a:person) WHERE date(2012) < 2 RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OrderByVariableNotInScope) {
    string expectedException = "Binder exception: Variable a is not in scope.";
    auto input = "MATCH (a:person)-[e1:knows]->(b:person) RETURN SUM(a.age) ORDER BY a;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, NestedAggregation) {
    string expectedException =
        "Binder exception: Expression SUM(SUM(a.age)) contains nested aggregation.";
    auto input = "MATCH (a:person) RETURN SUM(SUM(a.age));";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OptionalMatchAsFirstMatch) {
    string expectedException = "Binder exception: First match clause cannot be optional match.";
    auto input = "OPTIONAL MATCH (a:person) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, SubqueryWithAggregation1) {
    string expectedException =
        "Binder exception: Expression EXISTS { MATCH (a)-[:knows]->(b:person) RETURN COUNT(*) "
        "} is an existential subquery expression and should not contains "
        "any aggregation or order by in subquery RETURN or WITH clause.";
    auto input = "MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(b:person) RETURN COUNT(*) } "
                 "RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, SubqueryWithAggregation2) {
    string expectedException =
        "Binder exception: Expression EXISTS { MATCH (a)-[:knows]->(b:person) WITH COUNT(*) AS k "
        "RETURN k+1 } is an "
        "existential subquery expression and should not contains any aggregation or order by in "
        "subquery RETURN or WITH clause.";
    auto input = "MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(b:person) WITH COUNT(*) AS "
                 "k RETURN k+1 } "
                 "RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, SubqueryWithOrderBy) {
    string expectedException =
        "Binder exception: Expression EXISTS { MATCH (a)-[:knows]->(b:person) RETURN b.age ORDER "
        "BY b.age } is an existential subquery expression and should not "
        "contains any aggregation or order by in subquery RETURN or WITH clause.";
    auto input = "MATCH (a:person) WHERE EXISTS { MATCH (a)-[:knows]->(b:person) RETURN b.age "
                 "ORDER BY b.age } "
                 "RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, OrderByWithoutSkipLimitInWithClause) {
    string expectedException =
        "Binder exception: In WITH clause, ORDER BY must be followed by SKIP or LIMIT.";
    auto input = "MATCH (a:person) WITH a.age AS k ORDER BY k RETURN k";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAllUnmatchedNumberOfExpressions) {
    string expectedException =
        "Binder exception: The number of columns to union/union all must be the same.";
    auto input = "MATCH (p:person) RETURN p.age,p.name UNION ALL MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAllUnmatchedDataTypesOfExpressions) {
    string expectedException =
        "Binder exception: p1.age has data type INT64. (STRING) was expected.";
    auto input = "MATCH (p:person) RETURN p.name UNION ALL MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, UnionAndUnionAllInSingleQuery) {
    string expectedException = "Binder exception: Union and union all can't be used together.";
    auto input = "MATCH (p:person) RETURN p.age UNION ALL MATCH (p1:person) RETURN p1.age UNION "
                 "MATCH (p1:person) RETURN p1.age";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}

TEST_F(BinderErrorTest, VarLenExtendZeroLowerBound) {
    string expectedException =
        "Binder exception: Lower and upper bound of a rel must be greater than 0.";
    auto input = "MATCH (a:person)-[:knows*0..5]->(b:person) return count(*)";
    ASSERT_STREQ(expectedException.c_str(), getBindingError(input).c_str());
}
