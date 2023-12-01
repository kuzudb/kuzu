#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "common/assert.h"
#include "common/enums/expression_type.h"
#include "common/exception/internal.h"
#include "common/types/types.h"

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

    inline void setAlias(const std::string& name) { alias = name; }

    inline void setUniqueName(const std::string& name) { uniqueName = name; }
    inline std::string getUniqueName() const {
        KU_ASSERT(!uniqueName.empty());
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
    inline expression_vector getChildren() const { return children; }
    inline void setChild(common::vector_idx_t idx, std::shared_ptr<Expression> child) {
        children[idx] = std::move(child);
    }

    expression_vector splitOnAND();

    inline bool operator==(const Expression& rhs) const { return uniqueName == rhs.uniqueName; }

    std::string toString() const { return hasAlias() ? alias : toStringInternal(); }

    virtual std::unique_ptr<Expression> copy() const {
        throw common::InternalException("Unimplemented expression copy().");
    }

protected:
    virtual std::string toStringInternal() const = 0;

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

} // namespace binder
} // namespace kuzu
