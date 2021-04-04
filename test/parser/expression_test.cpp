#include "gtest/gtest.h"

#include "src/parser/include/parser.h"
#include "src/parser/include/parsed_expression.h"

using namespace graphflow::parser;

class ExpressionTest : public :: testing::Test {

public:
    unique_ptr<MatchStatement> makeBaseMatchStatement() {
        auto node = make_unique<NodePattern>(string(), string());
        auto patternElement = make_unique<PatternElement>(move(node));
        vector<unique_ptr<PatternElement>> patternElements;
        patternElements.push_back(move(patternElement));
        return make_unique<MatchStatement>(move(patternElements));
    }

    unique_ptr<ParsedExpression> makeAIsStudentExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "isStudent", string());
        expr->children.push_back(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeANameExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "name", string());
        expr->children.push_back(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeAAgeExpression() {
        auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "age", string());
        expr->children.push_back(move(aExpr));
        return expr;
    }

    unique_ptr<ParsedExpression> makeBIsMaleExpression() {
        auto bExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "b", string());
        auto expr = make_unique<ParsedExpression>(ExpressionType::PROPERTY, "isMale", string());
        expr->children.push_back(move(bExpr));
        return expr;
    }

};

TEST_F(ExpressionTest, VariablePropertyAndBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto bIsNotMale = make_unique<ParsedExpression>(ExpressionType::NOT, string(), string());
    bIsNotMale->children.push_back(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::AND, string(), string());
    expectedExpr->children.push_back(move(aIsStudent));
    expectedExpr->children.push_back(move(bIsNotMale));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND NOT b.isMale;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, NullOperatorAndMultiBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto aNameIsNull = make_unique<ParsedExpression>(ExpressionType::IS_NULL, string(), string());
    aNameIsNull->children.push_back(move(aName));
    auto leftAnd = make_unique<ParsedExpression>(ExpressionType::AND, string(), string());
    leftAnd->children.push_back(move(aIsStudent));
    leftAnd->children.push_back(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::AND, string(), string());
    expectedExpr->children.push_back(move(leftAnd));
    expectedExpr->children.push_back(move(aNameIsNull));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND b.isMale AND a.name IS NULL;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, StringOperatorAndMultiBooleanConnectionTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto xiyang = make_unique<ParsedExpression>(ExpressionType::LITERAL_STRING, "\"Xiyang\"", string());
    auto aNameContainsXiyang = make_unique<ParsedExpression>(ExpressionType::CONTAINS, string(), string());
    aNameContainsXiyang->children.push_back(move(aName));
    aNameContainsXiyang->children.push_back(move(xiyang));
    auto aIsStudentAndBIsMale = make_unique<ParsedExpression>(ExpressionType::AND, string(), string());
    aIsStudentAndBIsMale->children.push_back(move(aIsStudent));
    aIsStudentAndBIsMale->children.push_back(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::OR, string(), string());
    expectedExpr->children.push_back(move(aIsStudentAndBIsMale));
    expectedExpr->children.push_back(move(aNameContainsXiyang));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE (a.isStudent AND b.isMale) OR a.name CONTAINS \"Xiyang\";";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ArithmeticAndComparisonTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1", string());
    auto multiExpr = make_unique<ParsedExpression>(ExpressionType::MULTIPLY, string(), string(), move(a), move(pointOne));
    auto arithmeticExpr = make_unique<ParsedExpression>(ExpressionType::ADD, string(), string(), move(two), move(multiExpr));
    auto aAge = makeAAgeExpression();
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::EQUALS, string(), string());
    expectedExpr->children.push_back(move(arithmeticExpr));
    expectedExpr->children.push_back(move(aAge));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE (2 + a * 0.1) = a.age;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ArithmeticExpressionWithParenthesizeTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1", string());
    auto addExpr = make_unique<ParsedExpression>(ExpressionType::SUBTRACT, string(), string(), move(two), move(a));
    auto arithmeticExpr = make_unique<ParsedExpression>(ExpressionType::MODULO, string(), string(), move(addExpr), move(pointOne));
    auto aAge = makeAAgeExpression();
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::GREATER_THAN, string(), string());
    expectedExpr->children.push_back(move(arithmeticExpr));
    expectedExpr->children.push_back(move(aAge));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE ((2 - a) % 0.1) > a.age;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FunctionMultiParamsTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto bExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "b", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto bPowerTwo = make_unique<ParsedExpression>(ExpressionType::POWER, string(), string(), move(bExpr), move(two));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::FUNCTION, "MIN", string());
    expectedExpr->children.push_back(move(aExpr));
    expectedExpr->children.push_back(move(bPowerTwo));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->statements.push_back(move(expectedMatch));

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE MIN(a, b^2);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}
