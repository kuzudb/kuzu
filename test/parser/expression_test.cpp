#include "gtest/gtest.h"

#include "src/parser/expression/include/parsed_function_expression.h"
#include "src/parser/expression/include/parsed_literal_expression.h"
#include "src/parser/expression/include/parsed_property_expression.h"
#include "src/parser/expression/include/parsed_variable_expression.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;

const string EMPTY = string();

class ExpressionTest : public ::testing::Test {

public:
    static unique_ptr<ParsedExpression> makeAIsStudentExpression() {
        auto child = make_unique<ParsedVariableExpression>("a", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "isStudent", move(child), EMPTY);
    }

    static unique_ptr<ParsedExpression> makeANameExpression() {
        auto child = make_unique<ParsedVariableExpression>("a", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "name", move(child), EMPTY);
    }

    static unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto child = make_unique<ParsedVariableExpression>("a", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "age", move(child), EMPTY);
    }

    static unique_ptr<ParsedExpression> makeBIsMaleExpression() {
        auto child = make_unique<ParsedVariableExpression>("b", EMPTY);
        return make_unique<ParsedPropertyExpression>(PROPERTY, "isMale", move(child), EMPTY);
    }
};

TEST_F(ExpressionTest, FilterIDComparisonTest) {
    auto a = make_unique<ParsedVariableExpression>("a", EMPTY);
    auto aID = make_unique<ParsedFunctionExpression>("id", EMPTY);
    aID->addChild(move(a));
    auto b = make_unique<ParsedVariableExpression>("b", EMPTY);
    auto bID = make_unique<ParsedFunctionExpression>("id", EMPTY);
    bID->addChild(move(b));
    auto where = make_unique<ParsedExpression>(EQUALS, move(aID), move(bID), EMPTY);

    string input = "MATCH () WHERE id(a) = id(b) RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterBooleanConnectionTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto bIsNotMale = make_unique<ParsedExpression>(NOT, move(bIsMale), EMPTY);
    auto where = make_unique<ParsedExpression>(AND, move(aIsStudent), move(bIsNotMale), EMPTY);

    string input = "MATCH () WHERE a.isStudent AND NOT b.isMale RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterNullOperatorTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto aNameIsNull = make_unique<ParsedExpression>(IS_NULL, move(aName), EMPTY);
    auto leftAnd = make_unique<ParsedExpression>(AND, move(aIsStudent), move(bIsMale), EMPTY);
    auto where = make_unique<ParsedExpression>(AND, move(leftAnd), move(aNameIsNull), EMPTY);

    string input = "MATCH () WHERE a.isStudent AND b.isMale AND a.name IS NULL RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterStringOperatorTest) {
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aIsStudentAndBIsMale =
        make_unique<ParsedExpression>(AND, move(aIsStudent), move(bIsMale), EMPTY);
    auto aName = makeANameExpression();
    auto xiyang =
        make_unique<ParsedLiteralExpression>(make_unique<Literal>(string("Xiyang")), EMPTY);
    auto aNameContainsXiyang =
        make_unique<ParsedFunctionExpression>(CONTAINS_FUNC_NAME, move(aName), move(xiyang), EMPTY);
    auto where = make_unique<ParsedExpression>(
        OR, move(aIsStudentAndBIsMale), move(aNameContainsXiyang), EMPTY);

    string input =
        "MATCH () WHERE (a.isStudent AND b.isMale) OR a.name CONTAINS \"Xiyang\" RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterArithmeticComparisonTest) {
    auto a = make_unique<ParsedVariableExpression>("a", EMPTY);
    auto two = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)2), EMPTY);
    auto pointOne =
        make_unique<ParsedLiteralExpression>(make_unique<Literal>((double_t)0.1), EMPTY);
    auto multi =
        make_unique<ParsedFunctionExpression>(MULTIPLY_FUNC_NAME, move(a), move(pointOne), EMPTY);
    auto left = make_unique<ParsedFunctionExpression>(ADD_FUNC_NAME, move(two), move(multi), EMPTY);
    auto aAge = makeAAgeExpression();
    auto where = make_unique<ParsedExpression>(EQUALS, move(left), move(aAge), EMPTY);

    string input = "MATCH () WHERE 2 + a * 0.1 = a.age RETURN *";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterParenthesizeTest) {
    auto a = make_unique<ParsedVariableExpression>("a", EMPTY);
    auto two = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)2), EMPTY);
    auto pointOne =
        make_unique<ParsedLiteralExpression>(make_unique<Literal>((double_t)0.1), EMPTY);
    auto sub = make_unique<ParsedFunctionExpression>(SUBTRACT_FUNC_NAME, move(two), move(a), EMPTY);
    auto left =
        make_unique<ParsedFunctionExpression>(MODULO_FUNC_NAME, move(sub), move(pointOne), EMPTY);
    auto aAge = makeAAgeExpression();
    auto where = make_unique<ParsedExpression>(LESS_THAN_EQUALS, move(left), move(aAge), EMPTY);

    string input = "MATCH () WHERE ((2 - a) % 0.1) <= a.age RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}

TEST_F(ExpressionTest, FilterFunctionMultiParamsTest) {
    auto a = make_unique<ParsedVariableExpression>("a", EMPTY);
    auto b = make_unique<ParsedVariableExpression>("b", EMPTY);
    auto two = make_unique<ParsedLiteralExpression>(make_unique<Literal>((int64_t)2), EMPTY);
    auto bPowerTwo =
        make_unique<ParsedFunctionExpression>(POWER_FUNC_NAME, move(b), move(two), EMPTY);
    auto where = make_unique<ParsedFunctionExpression>("MIN", EMPTY);
    where->addChild(move(a));
    where->addChild(move(bPowerTwo));

    string input = "MATCH () WHERE MIN(a, b^2) RETURN *;";
    auto regularQuery = Parser::parseQuery(input);
    auto& matchClause = (MatchClause&)*regularQuery->getSingleQuery(0)->getMatchClause(0);
    ASSERT_TRUE(*where == *matchClause.getWhereClause());
}
