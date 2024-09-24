#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

struct ExpressionUtil {
    static expression_vector getExpressionsWithDataType(const expression_vector& expressions,
        common::LogicalTypeID dataTypeID);

    static uint32_t find(Expression* target, expression_vector expressions);

    // Print as a1,a2,a3,...
    static std::string toString(const expression_vector& expressions);
    static std::string toStringOrdered(const expression_vector& expressions);
    // Print as a1=a2, a3=a4,...
    static std::string toString(const std::vector<expression_pair>& expressionPairs);
    // Print as a1=a2
    static std::string toString(const expression_pair& expressionPair);
    static std::string getUniqueName(const expression_vector& expressions);

    static expression_vector excludeExpression(const expression_vector& exprs,
        const Expression& exprToExclude);
    static expression_vector excludeExpressions(const expression_vector& expressions,
        const expression_vector& expressionsToExclude);

    static common::logical_type_vec_t getDataTypes(const expression_vector& expressions);

    static expression_vector removeDuplication(const expression_vector& expressions);

    static bool isEmptyPattern(const Expression& expression);
    static bool isNodePattern(const Expression& expression);
    static bool isRelPattern(const Expression& expression);
    static bool isRecursiveRelPattern(const Expression& expression);
    static bool isNullLiteral(const Expression& expression);
    static bool isFalseLiteral(const Expression& expression);
    static bool isEmptyList(const Expression& expression);

    static void validateExpressionType(const Expression& expr, common::ExpressionType expectedType);

    // Validate data type.
    static void validateDataType(const Expression& expr, const common::LogicalType& expectedType);
    // Validate recursive data type top level (used when child type is unknown).
    static void validateDataType(const Expression& expr, common::LogicalTypeID expectedTypeID);
    static void validateDataType(const Expression& expr,
        const std::vector<common::LogicalTypeID>& expectedTypeIDs);
    template<typename T>
    static T getLiteralValue(const Expression& expr);

    static bool tryCombineDataType(const expression_vector& expressions,
        common::LogicalType& result);

    // Check If we can directly assign a new data type to an expression.
    // This mostly happen when a literal is an empty list. By default, we assign its data type to
    // INT64[] but it can be cast to any other list type at compile time.
    static bool canCastStatically(const Expression& expr, const common::LogicalType& targetType);
};

} // namespace binder
} // namespace kuzu
