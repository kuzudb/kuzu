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

    inline shared_ptr<Expression> removeAlias() const {
        assert(expressionType == ALIAS);
        return children[0]->expressionType == ALIAS ? children[0]->removeAlias() : children[0];
    }

    bool hasAggregationExpression() const;
    bool hasSubqueryExpression() const;

    unordered_set<string> getDependentVariableNames();

    virtual vector<shared_ptr<Expression>> getDependentVariables();
    virtual vector<shared_ptr<Expression>> getDependentProperties();
    virtual vector<shared_ptr<Expression>> getDependentLeafExpressions();
    vector<shared_ptr<Expression>> getDependentSubqueryExpressions();

    vector<shared_ptr<Expression>> splitOnAND();

public:
    ExpressionType expressionType;
    DataType dataType;
    string rawExpression;
    vector<shared_ptr<Expression>> children;
};

} // namespace binder
} // namespace graphflow
