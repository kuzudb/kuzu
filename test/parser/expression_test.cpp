#include "gtest/gtest.h"

#include "src/parser/include/parsed_expression.h"
#include "src/parser/include/parser.h"

using namespace graphflow::parser;

class ExpressionTest : public ::testing::Test {

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

TEST_F(ExpressionTest, ReturnCountStarTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto countStar = make_unique<ParsedExpression>(FUNCTION, "COUNT_STAR", "COUNT(*)");
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStatement =
        make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), false);
    returnStatement->expressions.push_back(move(countStar));
    expectedSingleQuery->returnStatement = move(returnStatement);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN COUNT(*);";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ReturnStarAndPropertyTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aName = makeANameExpression();
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStatement =
        make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    returnStatement->expressions.push_back(move(aName));
    expectedSingleQuery->returnStatement = move(returnStatement);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN *, a.name;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, ReturnAliasTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aAge = makeAAgeExpression();
    aAge->alias = "age";
    auto aName = makeANameExpression();
    aName->alias = "whatever";
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStatement =
        make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), false);
    returnStatement->expressions.push_back(move(aAge));
    returnStatement->expressions.push_back(move(aName));
    expectedSingleQuery->returnStatement = move(returnStatement);

    graphflow::parser::Parser parser;
    string input = "MATCH () RETURN a.age AS age, a.name AS whatever;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterBooleanConnectionTest) {
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
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND NOT b.isMale RETURN *;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterNullOperatorTest) {
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
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE a.isStudent AND b.isMale AND a.name IS NULL RETURN *;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterStringOperatorTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aIsStudent = makeAIsStudentExpression();
    auto bIsMale = makeBIsMaleExpression();
    auto aName = makeANameExpression();
    auto xiyang =
        make_unique<ParsedExpression>(ExpressionType::LITERAL_STRING, "\"Xiyang\"", string());
    auto aNameContainsXiyang =
        make_unique<ParsedExpression>(ExpressionType::CONTAINS, string(), string());
    aNameContainsXiyang->children.push_back(move(aName));
    aNameContainsXiyang->children.push_back(move(xiyang));
    auto aIsStudentAndBIsMale =
        make_unique<ParsedExpression>(ExpressionType::AND, string(), string());
    aIsStudentAndBIsMale->children.push_back(move(aIsStudent));
    aIsStudentAndBIsMale->children.push_back(move(bIsMale));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::OR, string(), string());
    expectedExpr->children.push_back(move(aIsStudentAndBIsMale));
    expectedExpr->children.push_back(move(aNameContainsXiyang));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input =
        "MATCH () WHERE (a.isStudent AND b.isMale) OR a.name CONTAINS \"Xiyang\" RETURN *;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterArithmeticComparisonTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1", string());
    auto multiExpr = make_unique<ParsedExpression>(
        ExpressionType::MULTIPLY, string(), string(), move(a), move(pointOne));
    auto arithmeticExpr = make_unique<ParsedExpression>(
        ExpressionType::ADD, string(), string(), move(two), move(multiExpr));
    auto aAge = makeAAgeExpression();
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::EQUALS, string(), string());
    expectedExpr->children.push_back(move(arithmeticExpr));
    expectedExpr->children.push_back(move(aAge));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE (2 + a * 0.1) = a.age RETURN *";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterParenthesizeTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto a = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto pointOne = make_unique<ParsedExpression>(ExpressionType::LITERAL_DOUBLE, "0.1", string());
    auto addExpr = make_unique<ParsedExpression>(
        ExpressionType::SUBTRACT, string(), string(), move(two), move(a));
    auto arithmeticExpr = make_unique<ParsedExpression>(
        ExpressionType::MODULO, string(), string(), move(addExpr), move(pointOne));
    auto aAge = makeAAgeExpression();
    auto expectedExpr =
        make_unique<ParsedExpression>(ExpressionType::GREATER_THAN, string(), string());
    expectedExpr->children.push_back(move(arithmeticExpr));
    expectedExpr->children.push_back(move(aAge));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE ((2 - a) % 0.1) > a.age RETURN *;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}

TEST_F(ExpressionTest, FilterFunctionMultiParamsTest) {
    auto expectedMatch = makeBaseMatchStatement();
    auto aExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "a", string());
    auto bExpr = make_unique<ParsedExpression>(ExpressionType::VARIABLE, "b", string());
    auto two = make_unique<ParsedExpression>(ExpressionType::LITERAL_INT, "2", string());
    auto bPowerTwo = make_unique<ParsedExpression>(
        ExpressionType::POWER, string(), string(), move(bExpr), move(two));
    auto expectedExpr = make_unique<ParsedExpression>(ExpressionType::FUNCTION, "MIN", string());
    expectedExpr->children.push_back(move(aExpr));
    expectedExpr->children.push_back(move(bPowerTwo));
    expectedMatch->whereClause = move(expectedExpr);
    auto expectedSingleQuery = make_unique<SingleQuery>();
    expectedSingleQuery->matchStatements.push_back(move(expectedMatch));
    auto returnStar = make_unique<ReturnStatement>(vector<unique_ptr<ParsedExpression>>(), true);
    expectedSingleQuery->returnStatement = move(returnStar);

    graphflow::parser::Parser parser;
    string input = "MATCH () WHERE MIN(a, b^2) RETURN *;";
    ASSERT_TRUE(*parser.parseQuery(input) == *expectedSingleQuery);
}
