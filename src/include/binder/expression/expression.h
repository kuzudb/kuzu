#pragma once

#include <cassert>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "common/exception.h"
#include "common/expression_type.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace binder {

class Expression;
using expression_vector = std::vector<std::shared_ptr<Expression>>;
using expression_pair = std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>;

struct ExpressionHasher;
struct ExpressionEquality;
using expression_set =
    std::unordered_set<std::shared_ptr<Expression>, ExpressionHasher, ExpressionEquality>;
template<typename T>
using expression_map =
    std::unordered_map<std::shared_ptr<Expression>, T, ExpressionHasher, ExpressionEquality>;

class Expression : public std::enable_shared_from_this<Expression> {
    friend class ExpressionChildrenCollector;

public:
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        expression_vector children, std::string uniqueName)
        : expressionType{expressionType}, dataType{std::move(dataType)},
          uniqueName{std::move(uniqueName)}, children{std::move(children)} {}

    // Create binary expression.
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right,
        std::string uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{left, right},
              std::move(uniqueName)} {}

    // Create unary expression.
    Expression(common::ExpressionType expressionType, common::LogicalType dataType,
        const std::shared_ptr<Expression>& child, std::string uniqueName)
        : Expression{expressionType, std::move(dataType), expression_vector{child},
              std::move(uniqueName)} {}

    // Create leaf expression
    Expression(
        common::ExpressionType expressionType, common::LogicalType dataType, std::string uniqueName)
        : Expression{
              expressionType, std::move(dataType), expression_vector{}, std::move(uniqueName)} {}

    virtual ~Expression() = default;

public:
    inline void setAlias(const std::string& name) { alias = name; }

    inline std::string getUniqueName() const {
        assert(!uniqueName.empty());
        return uniqueName;
    }

    inline common::LogicalType getDataType() const { return dataType; }
    inline common::LogicalType& getDataTypeReference() { return dataType; }

    inline bool hasAlias() const { return !alias.empty(); }

    inline std::string getAlias() const { return alias; }

    inline uint32_t getNumChildren() const { return children.size(); }

    inline std::shared_ptr<Expression> getChild(common::vector_idx_t idx) const {
        return children[idx];
    }
    inline void setChild(common::vector_idx_t idx, std::shared_ptr<Expression> child) {
        children[idx] = std::move(child);
    }

    expression_vector splitOnAND();

    inline bool operator==(const Expression& rhs) const { return uniqueName == rhs.uniqueName; }

    virtual std::string toString() const = 0;

    virtual std::unique_ptr<Expression> copy() const {
        throw common::InternalException("Unimplemented expression copy().");
    }

public:
    common::ExpressionType expressionType;
    common::LogicalType dataType;

protected:
    // Name that serves as the unique identifier.
    std::string uniqueName;
    std::string alias;
    expression_vector children;
};

struct ExpressionHasher {
    std::size_t operator()(const std::shared_ptr<Expression>& expression) const {
        return std::hash<std::string>{}(expression->getUniqueName());
    }
};

struct ExpressionEquality {
    bool operator()(
        const std::shared_ptr<Expression>& left, const std::shared_ptr<Expression>& right) const {
        return left->getUniqueName() == right->getUniqueName();
    }
};

struct ExpressionUtil {
    static bool isExpressionsWithDataType(
        const expression_vector& expressions, common::LogicalTypeID dataTypeID);
    static expression_vector getExpressionsWithDataType(
        const expression_vector& expressions, common::LogicalTypeID dataTypeID);

    static uint32_t find(Expression* target, expression_vector expressions);

    // Print as a1,a2,a3,...
    static std::string toString(const expression_vector& expressions);
    // Print as a1=a2, a3=a4,...
    static std::string toString(const std::vector<expression_pair>& expressionPairs);
    // Print as a1=a2
    static std::string toString(const expression_pair& expressionPair);

    static expression_vector excludeExpressions(
        const expression_vector& expressions, const expression_vector& expressionsToExclude);

    inline static bool isNodeVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::NODE;
    }
    inline static bool isRelVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::REL;
    }
    inline static bool isRecursiveRelVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::RECURSIVE_REL;
    }

    static std::vector<std::unique_ptr<common::LogicalType>> getDataTypes(
        const expression_vector& expressions);
};

} // namespace binder
} // namespace kuzu
