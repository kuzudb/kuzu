#include "gtest/gtest.h"

#include "src/parser/include/parser.h"
#include "src/parser/include/parsed_expression.h"

using namespace graphflow::parser;

class ExpressionTest : public :: testing::Test {

public:
    unique_ptr<MatchStatement> makeBaseMatchStatement() {
        auto node = make_unique<NodePattern>();
        auto patternElement = make_unique<PatternElement>();
        patternElement->setNodePattern(move(node));
        vector<unique_ptr<PatternElement>> patternElements;
        patternElements.push_back(move(patternElement));
        return make_unique<MatchStatement>(move(patternElements));
    }

    unique_ptr<ParsedExpression> makeAIsStudentExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "isStudent");
        expr->addChild(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeANameExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "name");
        expr->addChild(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "age");
        expr->addChild(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeBIsMaleExpression() {
        auto bExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "b");
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "isMale");
        expr->addChild(move(bExpr));
        return expr;
    }

};

TEST_F(ExpressionTest, VariablePropertyAndBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto bIsNotMale = make_unique<ParsedExpression>(ExpressionType::NOT);
    bIsNotMale->addChild(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::AND);
    expectedExpr->addChild(move(aIsStudent));
    expectedExpr->addChild(move(bIsNotMale));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND NOT b.isMale;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, NullOperatorAndMultiBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto aNameIsNull = make_unique<ParsedExpression>(ExpressionType::IS_NULL);
    aNameIsNull->addChild(move(aName));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::AND);
    expectedExpr->addChild(move(aIsStudent));
    expectedExpr->addChild(move(bIsMale));
    expectedExpr->addChild(move(aNameIsNull));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND b.isMale AND a.name IS NULL;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, StringOperatorAndMultiBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto xiyang = make_unique<ParsedExpression>(ExpressionType::LITERAL_STRING, "\"Xiyang\"");
    auto aNameContainsXiyang = make_unique<ParsedExpression>(ExpressionType::CONTAINS);
    aNameContainsXiyang->addChild(move(aName));
    aNameContainsXiyang->addChild(move(xiyang));
    auto aIsStudentAndBIsMale = make_unique<ParsedExpression>(ExpressionType::AND);
    aIsStudentAndBIsMale->addChild(move(aIsStudent));
    aIsStudentAndBIsMale->addChild(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::OR);
    expectedExpr->addChild(move(aIsStudentAndBIsMale));
    expectedExpr->addChild(move(aNameContainsXiyang));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE (a.isStudent AND b.isMale) OR a.name CONTAINS \"Xiyang\";";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ArithmeticAndComparisonTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2");
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1");
    auto multiExpr = make_unique<ParsedExpression>(ExpressionType::MULTIPLY, move(a), move(pointOne));
    auto arithmeticExpr = make_unique<ParsedExpression>(ExpressionType::ADD, move(two), move(multiExpr));
    auto aAge = makeAAgeExpression();
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::EQUALS);
    expectedExpr->addChild(move(arithmeticExpr));
    expectedExpr->addChild(move(aAge));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE (2 + a * 0.1) = a.age;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ArithmeticExpressionWithParenthesizeTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2");
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1");
    auto addExpr = make_unique<ParsedExpression>(ExpressionType::SUBTRACT, move(two), move(a));
    auto arithmeticExpr = make_unique<ParsedExpression>(ExpressionType::MODULO, move(addExpr), move(pointOne));
    auto aAge = makeAAgeExpression();
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::GREATER_THAN);
    expectedExpr->addChild(move(arithmeticExpr));
    expectedExpr->addChild(move(aAge));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE ((2 - a) % 0.1) > a.age;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FunctionMultiParamsTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a");
    auto bExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "b");
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2");
    auto bPowerTwo = make_unique<ParsedExpression>(ExpressionType::POWER, move(bExpr), move(two));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::FUNCTION, "MIN");
    expectedExpr->addChild(move(aExpr));
    expectedExpr->addChild(move(bPowerTwo));
    expectedMatch->setWhereClause(move(expectedExpr));
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->addMatchStatement(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE MIN(a, b^2);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}
