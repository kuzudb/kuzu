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

class Expression {

public:
    // Create binary expression.
    Expression(ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& left,
        const shared_ptr<Expression>& right);

    // Create unary expression.
    Expression(
        ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& child);

    vector<const Expression*> getIncludedPropertyExpressions() const {
        return getIncludedExpressionsWithTypes(unordered_set<ExpressionType>{PROPERTY});
    }

    vector<const Expression*> getIncludedPropertyOrCSVLineExtractExpressions() const {
        return getIncludedExpressionsWithTypes(
            unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT});
    }

    unordered_set<string> getIncludedVariableNames() const;

    // return named used for parsing user input and printing
    virtual inline string getExternalName() const { return rawExpression; }

    // return named used in internal processing
    // alias name should never be used in internal processing
    virtual inline string getInternalName() const {
        return ALIAS == expressionType ? children[0]->getInternalName() : rawExpression;
    }

protected:
    Expression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

private:
    vector<const Expression*> getIncludedExpressionsWithTypes(
        const unordered_set<ExpressionType>& expressionTypes) const;

public:
    ExpressionType expressionType;
    DataType dataType;
    string rawExpression;
    vector<shared_ptr<Expression>> children;
};

} // namespace binder
} // namespace graphflow
