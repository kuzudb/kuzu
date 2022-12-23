#pragma once

#include <cassert>
#include <functional>
#include <memory>
#include <unordered_set>

#include "common/expression_type.h"
#include "common/types/types_include.h"

using namespace kuzu::common;
using namespace std;

namespace kuzu {
namespace binder {

class Expression;
using expression_vector = vector<shared_ptr<Expression>>;
using expression_pair = pair<shared_ptr<Expression>, shared_ptr<Expression>>;

class Expression : public enable_shared_from_this<Expression> {

public:
    Expression(ExpressionType expressionType, DataType dataType, expression_vector children,
        string uniqueName)
        : expressionType{expressionType}, dataType{move(dataType)},
          uniqueName{move(uniqueName)}, children{move(children)} {}

    // Create binary expression.
    Expression(ExpressionType expressionType, DataType dataType, const shared_ptr<Expression>& left,
        const shared_ptr<Expression>& right, const string& uniqueName)
        : Expression{expressionType, move(dataType), expression_vector{left, right}, uniqueName} {}

    // Create unary expression.
    Expression(ExpressionType expressionType, DataType dataType,
        const shared_ptr<Expression>& child, const string& uniqueName)
        : Expression{expressionType, move(dataType), expression_vector{child}, uniqueName} {}

    // Create leaf expression
    Expression(ExpressionType expressionType, DataType dataType, const string& uniqueName)
        : Expression{expressionType, move(dataType), expression_vector{}, uniqueName} {}

    virtual ~Expression() = default;

protected:
    Expression(ExpressionType expressionType, DataTypeID dataTypeID, const string& uniqueName)
        : Expression{expressionType, DataType(dataTypeID), uniqueName} {
        assert(dataTypeID != LIST);
    }

public:
    inline void setAlias(const string& name) { alias = name; }

    inline void setRawName(const string& name) { rawName = name; }

    inline string getUniqueName() const {
        assert(!uniqueName.empty());
        return uniqueName;
    }

    inline DataType getDataType() const { return dataType; }

    inline bool hasAlias() const { return !alias.empty(); }

    inline string getAlias() const { return alias; }

    inline string getRawName() const { return rawName; }

    inline uint32_t getNumChildren() const { return children.size(); }

    inline shared_ptr<Expression> getChild(uint32_t idx) const { return children[idx]; }

    inline virtual expression_vector getChildren() const { return children; }

    bool hasAggregationExpression() const { return hasSubExpressionOfType(isExpressionAggregate); }

    bool hasSubqueryExpression() const { return hasSubExpressionOfType(isExpressionSubquery); }

    unordered_set<string> getDependentVariableNames();

    expression_vector getSubPropertyExpressions();

    expression_vector getTopLevelSubSubqueryExpressions();

    expression_vector splitOnAND();

protected:
    bool hasSubExpressionOfType(
        const std::function<bool(ExpressionType type)>& typeCheckFunc) const;

public:
    ExpressionType expressionType;
    DataType dataType;

protected:
    // Name that serves as the unique identifier.
    // NOTE: an expression must have a unique name
    string uniqueName;
    string alias;
    // Name that matches user input.
    // NOTE: an expression may not have a rawName since it is generated internally e.g. casting
    string rawName;
    expression_vector children;
};

class ExpressionUtil {
public:
    static bool allExpressionsHaveDataType(expression_vector& expressions, DataTypeID dataTypeID);

    static uint32_t find(Expression* target, expression_vector expressions);

    static string toString(const expression_vector& expressions);
};

} // namespace binder
} // namespace kuzu
