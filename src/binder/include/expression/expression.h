#pragma once

#include <cassert>
#include <memory>
#include <unordered_set>

#include "src/common/include/expression_type.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace binder {

// replace this with function enum once we have more functions
const string FUNCTION_COUNT_STAR = "COUNT_STAR";
const string FUNCTION_ID = "ID";

class Expression {

public:
    // creates a non-leaf logical binary expression.
    Expression(ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& left,
        const shared_ptr<Expression>& right);

    // creates a non-leaf logical unary expression.
    Expression(
        ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& child);

    Expression(ExpressionType expressionType, DataType dataType, const string& variableName);

    virtual void cast(DataType dataTypeToCast) {
        throw invalid_argument("Expression does not support cast(DataType).");
    }

    inline const Expression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

    inline const string getAliasElseRawExpression() const {
        return alias.empty() ? rawExpression : alias;
    }

    vector<const Expression*> getIncludedPropertyExpressions() const {
        return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{PROPERTY});
    }

    vector<const Expression*> getIncludedLeafVariableExpressions() const {
        return getIncludedExpressionsWithTypes(
            unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT});
    }

    unordered_set<string> getIncludedVariableNames() const;

protected:
    Expression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

private:
    vector<const Expression*> getIncludedExpressionsWithTypes(
        const unordered_set<ExpressionType>& expressionTypes) const;

public:
    // variable name for leaf variable expressions.
    string variableName;
    vector<shared_ptr<Expression>> childrenExpr;
    ExpressionType expressionType;
    DataType dataType;
    string alias;
    string rawExpression;
};

} // namespace binder
} // namespace graphflow
