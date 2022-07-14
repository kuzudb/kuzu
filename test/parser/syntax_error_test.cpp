#include "gtest/gtest.h"

#include "src/common/include/exception.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;
using namespace graphflow::common;
using namespace ::testing;

class SyntaxErrorTest : public Test {

public:
    static string getParsingError(const string& input) {
        try {
            Parser::parseQuery(input);
        } catch (const Exception& exception) { return exception.what(); }
        return string();
    }
};

TEST_F(SyntaxErrorTest, QueryNotConcludeWithReturn1) {
    string expectedException =
        "Parser exception: Query must conclude with RETURN clause (line: 1, offset: 0)\n";
    expectedException += "\"MATCH (a:person);\"\n";
    expectedException += " ^^^^^";
    auto input = "MATCH (a:person);";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, QueryNotConcludeWithReturn2) {
    string expectedException =
        "Parser exception: Query must conclude with RETURN clause (line: 1, offset: 23)\n";
    expectedException += "\"MATCH (a:person) WITH *;\"\n";
    expectedException += "                        ^";
    auto input = "MATCH (a:person) WITH *;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, QueryNodeWithOutParentheses) {
    string expectedException = "Parser exception: Parentheses are required to identify nodes in "
                               "patterns, i.e. (a) (line: 1, offset: 6)\n";
    expectedException += "\"MATCH a RETURN *;\"\n";
    expectedException += "       ^";
    auto input = "MATCH a RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, InvalidNotEqualOperator) {
    string expectedException =
        "Parser exception: Unknown operation '!=' (you probably meant to use '<>', which is "
        "the operator for inequality testing.) (line: 1, offset: 22)\n";
    expectedException += "\"MATCH (a) WHERE a.age != 1 RETURN *;\"\n";
    expectedException += "                       ^^";
    auto input = "MATCH (a) WHERE a.age != 1 RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, EmptyLabel) {
    string expectedException =
        "Parser exception: '' is not a valid token name. Token names cannot be empty or "
        "contain any null-bytes (line: 1, offset: 9)\n";
    expectedException += "\"MATCH (a:``) RETURN *;\"\n";
    expectedException += "          ^^";
    auto input = "MATCH (a:``) RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, EmptyProperty) {
    string expectedException =
        "Parser exception: '' is not a valid token name. Token names cannot be empty or "
        "contain any null-bytes (line: 1, offset: 18)\n";
    expectedException += "\"MATCH (a) WHERE a.`` != 1 RETURN *;\"\n";
    expectedException += "                   ^^";
    auto input = "MATCH (a) WHERE a.`` != 1 RETURN *;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, ReturnNotAtEnd) {
    string expectedException =
        "Parser exception: RETURN can only be used at the end of the query (line: 1, offset: 0)\n";
    expectedException += "\"RETURN a MATCH (a) RETURN a;\"\n";
    expectedException += " ^^^^^^";
    auto input = "RETURN a MATCH (a) RETURN a;";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}

TEST_F(SyntaxErrorTest, ConjunctiveComparison) {
    string expectedException = "Parser exception: Non-binary comparison (e.g. a=b=c) is not "
                               "supported (line: 1, offset: 69)\n";
    expectedException += "\"MATCH (a:person)<-[e1:knows]-(b:person)-[e2:knows]->(c:person) WHERE "
                         "b.fName = e1.date = e2.date AND id(a) <> id(c) RETURN COUNT(*);\"\n";
    expectedException += "                                                                      ^";
    auto input = "MATCH (a:person)<-[e1:knows]-(b:person)-[e2:knows]->(c:person) WHERE b.fName = "
                 "e1.date = e2.date AND id(a) <> id(c) RETURN COUNT(*);";
    ASSERT_STREQ(expectedException.c_str(), getParsingError(input).c_str());
}
