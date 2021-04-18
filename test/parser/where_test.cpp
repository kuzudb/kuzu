#include "gtest/gtest.h"
#include "test/parser/parser_test_utils.h"

#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class WhereTest : public ::testing::Test {

public:
    static unique_ptr<ParsedExpression> makeAIsStudentExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "isStudent", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "a", EMPTY));
        return expression;
    }

    static unique_ptr<ParsedExpression> makeANameExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "name", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "a", EMPTY));
        return expression;
    }

    static unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "age", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "a", EMPTY));
        return expression;
    }

    static unique_ptr<ParsedExpression> makeBIsMaleExpression() {
        auto expression = make_unique<ParsedExpression>(PROPERTY, "isMale", EMPTY);
        expression->children.push_back(make_unique<ParsedExpression>(VARIABLE, "b", EMPTY));
        return expression;
    }
};

TEST_F(WhereTest, FilterIDComparisonTest) {
    auto a = make_unique<ParsedExpression>(VARIABLE, "a", EMPTY);
    auto aID = make_unique<ParsedExpression>(FUNCTION, "id", EMPTY);
    aID->children.push_back(move(a));
    auto b = make_unique<ParsedExpression>(VARIABLE, "b", EMPTY);
    auto bID = make_unique<ParsedExpression>(FUNCTION, "id", EMPTY);
    bID->children.push_back(move(b));
    auto where = make_unique<ParsedExpression>(EQUALS, EMPTY, EMPTY, move(aID), move(bID));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE id(a) = id(b) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterBooleanConnectionTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto bIsNotMale = make_unique<ParsedExpression>(NOT, EMPTY, EMPTY);
    bIsNotMale->children.push_back(move(bIsMale));
    auto where = make_unique<ParsedExpression>(AND, EMPTY, EMPTY);
    where->children.push_back(move(aIsStudent));
    where->children.push_back(move(bIsNotMale));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND NOT b.isMale RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterNullOperatorTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto aNameIsNull = make_unique<ParsedExpression>(IS_NULL, EMPTY, EMPTY);
    aNameIsNull->children.push_back(move(aName));
    auto leftAnd =
        make_unique<ParsedExpression>(AND, EMPTY, EMPTY, move(aIsStudent), move(bIsMale));
    auto where = make_unique<ParsedExpression>(AND, EMPTY, EMPTY, move(leftAnd), move(aNameIsNull));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND b.isMale AND a.name IS NULL RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterStringOperatorTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aIsStudentAndBIsMale =
        make_unique<ParsedExpression>(AND, EMPTY, EMPTY, move(aIsStudent), move(bIsMale));
    auto aName = makeANameExpression();
    auto xiyang = make_unique<ParsedExpression>(LITERAL_STRING, "\"Xiyang\"", EMPTY);
    auto aNameContainsXiyang =
        make_unique<ParsedExpression>(CONTAINS, EMPTY, EMPTY, move(aName), move(xiyang));
    auto where = make_unique<ParsedExpression>(
        OR, EMPTY, EMPTY, move(aIsStudentAndBIsMale), move(aNameContainsXiyang));

    graphflow::parser::Parser parser;
    string input =
        "MATCH () WHERE (a.isStudent AND b.isMale) OR a.name CONTAINS \"Xiyang\" RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterArithmeticComparisonTest) {
    auto a = make_unique<ParsedExpression>(VARIABLE, "a", EMPTY);
    auto two = make_unique<ParsedExpression>(LITERAL_INT, "2", EMPTY);
    auto pointOne = make_unique<ParsedExpression>(LITERAL_DOUBLE, "0.1", EMPTY);
    auto multi = make_unique<ParsedExpression>(MULTIPLY, EMPTY, EMPTY, move(a), move(pointOne));
    auto left = make_unique<ParsedExpression>(ADD, EMPTY, EMPTY, move(two), move(multi));
    auto aAge = makeAAgeExpression();
    auto where = make_unique<ParsedExpression>(EQUALS, EMPTY, EMPTY, move(left), move(aAge));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE 2 + a * 0.1 = a.age RETURN *";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterParenthesizeTest) {
    auto a = make_unique<ParsedExpression>(VARIABLE, "a", EMPTY);
    auto two = make_unique<ParsedExpression>(LITERAL_INT, "2", EMPTY);
    auto pointOne = make_unique<ParsedExpression>(LITERAL_DOUBLE, "0.1", EMPTY);
    auto sub = make_unique<ParsedExpression>(SUBTRACT, EMPTY, EMPTY, move(two), move(a));
    auto left = make_unique<ParsedExpression>(MODULO, EMPTY, EMPTY, move(sub), move(pointOne));
    auto aAge = makeAAgeExpression();
    auto where =
        make_unique<ParsedExpression>(LESS_THAN_EQUALS, EMPTY, EMPTY, move(left), move(aAge));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE ((2 - a) % 0.1) <= a.age RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}

TEST_F(WhereTest, FilterFunctionMultiParamsTest) {
    auto a = make_unique<ParsedExpression>(VARIABLE, "a", EMPTY);
    auto b = make_unique<ParsedExpression>(VARIABLE, "b", EMPTY);
    auto two = make_unique<ParsedExpression>(LITERAL_INT, "2", EMPTY);
    auto bPowerTwo = make_unique<ParsedExpression>(POWER, EMPTY, EMPTY, move(b), move(two));
    auto where = make_unique<ParsedExpression>(FUNCTION, "MIN", EMPTY);
    where->children.push_back(move(a));
    where->children.push_back(move(bPowerTwo));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE MIN(a, b^2) RETURN *;";
    auto singleQuery = parser.parseQuery(input);
    ASSERT_TRUE(ParserTestUtils::equals(*where, *singleQuery->matchStatements[0]->whereClause));
}
