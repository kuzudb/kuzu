#pragma once

#include <cassert>
#include <functional>
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

    // Create leaf expression
    Expression(ExpressionType expressionType, DataType dataType, const string& name);

    inline void setAlias(const string& name) { alias = name; }
    inline void setRawName(const string& name) { rawName = name; }

    inline string getUniqueName() const {
        assert(!uniqueName.empty());
        return uniqueName;
    }
    inline string getAlias() const { return alias; }
    inline string getRawName() const { return rawName; }

    bool hasAggregationExpression() const { return hasSubExpressionOfType(isExpressionAggregate); }
    bool hasSubqueryExpression() const { return hasSubExpressionOfType(isExpressionSubquery); }

    unordered_set<string> getDependentVariableNames();

    virtual vector<shared_ptr<Expression>> getSubVariableExpressions();
    vector<shared_ptr<Expression>> getTopLevelSubAggregationExpressions() {
        return getTopLevelSubExpressionsOfType(isExpressionAggregate);
    }
    vector<shared_ptr<Expression>> getTopLevelSubSubqueryExpressions() {
        return getTopLevelSubExpressionsOfType(isExpressionSubquery);
    }

    vector<shared_ptr<Expression>> splitOnAND();

protected:
    Expression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

    vector<shared_ptr<Expression>> getTopLevelSubExpressionsOfType(
        const std::function<bool(ExpressionType type)>& typeCheckFunc);

    bool hasSubExpressionOfType(
        const std::function<bool(ExpressionType type)>& typeCheckFunc) const;

public:
    ExpressionType expressionType;
    DataType dataType;
    // Name that serves as the unique identifier.
    // NOTE: an expression must have an unique name
    string uniqueName;
    string alias;
    // Name that matches user input.
    // NOTE: an expression may not have a rawName since it is generated internally e.g. casting
    string rawName;
    vector<shared_ptr<Expression>> children;
};

} // namespace binder
} // namespace graphflow
