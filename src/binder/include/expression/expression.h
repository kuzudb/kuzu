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

class Expression : public enable_shared_from_this<Expression> {

public:
    // Create binary expression.
    Expression(ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& left,
        const shared_ptr<Expression>& right);

    // Create unary expression.
    Expression(
        ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& child);

    // Create function expression with no parameter
    Expression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

    // return named used for parsing user input and printing
    virtual inline string getExternalName() const { return rawExpression; }

    // return named used in internal processing
    // alias name should never be used in internal processing
    virtual inline string getInternalName() const {
        return ALIAS == expressionType ? children[0]->getInternalName() : rawExpression;
    }

    unordered_set<string> getDependentVariableNames();

    vector<shared_ptr<Expression>> getDependentProperties() {
        return getDependentExpressionsWithTypes(unordered_set<ExpressionType>{PROPERTY});
    }
    vector<shared_ptr<Expression>> getDependentLeafExpressions() {
        return getDependentExpressionsWithTypes(
            unordered_set<ExpressionType>{PROPERTY, CSV_LINE_EXTRACT, ALIAS});
    }

private:
    vector<shared_ptr<Expression>> getDependentVariables() {
        return getDependentExpressionsWithTypes(unordered_set<ExpressionType>{VARIABLE});
    }

    /**
     * This function collects top-level subexpression which satisfies input expression types.
     * E.g. getDependentExpressionsWithTypes({PROPERTY, VARIABLE}) will only return property
     * expression "a.age" but not variable expression "a".
     */
    vector<shared_ptr<Expression>> getDependentExpressionsWithTypes(
        const unordered_set<ExpressionType>& expressionTypes);

public:
    ExpressionType expressionType;
    DataType dataType;
    string rawExpression;
    vector<shared_ptr<Expression>> children;
};

} // namespace binder
} // namespace graphflow
