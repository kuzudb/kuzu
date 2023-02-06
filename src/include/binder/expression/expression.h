#pragma once

#include <cassert>
#include <functional>
#include <memory>
#include <unordered_set>

#include "common/exception.h"
#include "common/expression_type.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace binder {

class Expression;
using expression_vector = std::vector<std::shared_ptr<Expression>>;
using expression_pair = std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>;

class Expression : public std::enable_shared_from_this<Expression> {
public:
    Expression(common::ExpressionType expressionType, common::DataType dataType,
        expression_vector children, std::string uniqueName)
        : expressionType{expressionType}, dataType{std::move(dataType)},
          uniqueName{std::move(uniqueName)}, children{std::move(children)} {}

    // Create binary expression.
    Expression(common::ExpressionType expressionType, common::DataType dataType,
        const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right,
        const std::string& uniqueName)
        : Expression{
              expressionType, std::move(dataType), expression_vector{left, right}, uniqueName} {}

    // Create unary expression.
    Expression(common::ExpressionType expressionType, common::DataType dataType,
        const std::shared_ptr<Expression>& child, const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{child}, uniqueName} {}

    // Create leaf expression
    Expression(common::ExpressionType expressionType, common::DataType dataType,
        const std::string& uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{}, uniqueName} {}

    virtual ~Expression() = default;

protected:
    Expression(common::ExpressionType expressionType, common::DataTypeID dataTypeID,
        const std::string& uniqueName)
        : Expression{expressionType, common::DataType(dataTypeID), uniqueName} {
        assert(dataTypeID != common::LIST);
    }

public:
    inline void setAlias(const std::string& name) { alias = name; }

    inline void setRawName(const std::string& name) { rawName = name; }

    inline std::string getUniqueName() const {
        assert(!uniqueName.empty());
        return uniqueName;
    }

    inline common::DataType getDataType() const { return dataType; }

    inline bool hasAlias() const { return !alias.empty(); }

    inline std::string getAlias() const { return alias; }

    inline std::string getRawName() const { return rawName; }

    inline uint32_t getNumChildren() const { return children.size(); }

    inline std::shared_ptr<Expression> getChild(uint32_t idx) const { return children[idx]; }

    inline virtual expression_vector getChildren() const { return children; }

    bool hasAggregationExpression() const {
        return hasSubExpressionOfType(common::isExpressionAggregate);
    }

    bool hasSubqueryExpression() const {
        return hasSubExpressionOfType(common::isExpressionSubquery);
    }

    std::unordered_set<std::string> getDependentVariableNames();

    expression_vector getSubPropertyExpressions();

    expression_vector getTopLevelSubSubqueryExpressions();

    expression_vector splitOnAND();

    virtual std::unique_ptr<Expression> copy() const {
        throw common::InternalException("Unimplemented expression copy().");
    }

protected:
    bool hasSubExpressionOfType(
        const std::function<bool(common::ExpressionType type)>& typeCheckFunc) const;

public:
    common::ExpressionType expressionType;
    common::DataType dataType;

protected:
    // Name that serves as the unique identifier.
    // NOTE: an expression must have a unique name
    std::string uniqueName;
    std::string alias;
    // Name that matches user input.
    // NOTE: an expression may not have a rawName since it is generated internally e.g. casting
    std::string rawName;
    expression_vector children;
};

class ExpressionUtil {
public:
    static bool allExpressionsHaveDataType(
        expression_vector& expressions, common::DataTypeID dataTypeID);

    static uint32_t find(Expression* target, expression_vector expressions);

    static std::string toString(const expression_vector& expressions);
};

} // namespace binder
} // namespace kuzu
