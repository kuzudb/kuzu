#include "binder/expression/expression_util.h"

#include <algorithm>

#include "binder/expression/literal_expression.h"
#include "binder/expression/node_rel_expression.h"
#include "binder/expression/parameter_expression.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector ExpressionUtil::getExpressionsWithDataType(const expression_vector& expressions,
    common::LogicalTypeID dataTypeID) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() == dataTypeID) {
            result.push_back(expression);
        }
    }
    return result;
}

uint32_t ExpressionUtil::find(Expression* target, expression_vector expressions) {
    for (auto i = 0u; i < expressions.size(); ++i) {
        if (target->getUniqueName() == expressions[i]->getUniqueName()) {
            return i;
        }
    }
    return INVALID_IDX;
}

std::string ExpressionUtil::toString(const expression_vector& expressions) {
    if (expressions.empty()) {
        return std::string{};
    }
    auto result = expressions[0]->toString();
    for (auto i = 1u; i < expressions.size(); ++i) {
        result += "," + expressions[i]->toString();
    }
    return result;
}

std::string ExpressionUtil::toStringOrdered(const expression_vector& expressions) {
    auto expressions_ = expressions;
    std::sort(expressions_.begin(), expressions_.end(),
        [](const std::shared_ptr<binder::Expression>& a,
            const std::shared_ptr<binder::Expression>& b) {
            return a->toString() < b->toString();
        });
    return toString(expressions_);
}

std::string ExpressionUtil::toString(const std::vector<expression_pair>& expressionPairs) {
    if (expressionPairs.empty()) {
        return std::string{};
    }
    auto result = toString(expressionPairs[0]);
    for (auto i = 1u; i < expressionPairs.size(); ++i) {
        result += "," + toString(expressionPairs[i]);
    }
    return result;
}

std::string ExpressionUtil::toString(const expression_pair& expressionPair) {
    return expressionPair.first->toString() + "=" + expressionPair.second->toString();
}

std::string ExpressionUtil::getUniqueName(const expression_vector& expressions) {
    if (expressions.empty()) {
        return std::string();
    }
    auto result = expressions[0]->getUniqueName();
    for (auto i = 1u; i < expressions.size(); ++i) {
        result += "," + expressions[i]->getUniqueName();
    }
    return result;
}

expression_vector ExpressionUtil::excludeExpression(const expression_vector& exprs,
    const Expression& exprToExclude) {
    expression_vector result;
    for (auto& expr : exprs) {
        if (*expr != exprToExclude) {
            result.push_back(expr);
        }
    }
    return result;
}

expression_vector ExpressionUtil::excludeExpressions(const expression_vector& expressions,
    const expression_vector& expressionsToExclude) {
    expression_set excludeSet;
    for (auto& expression : expressionsToExclude) {
        excludeSet.insert(expression);
    }
    expression_vector result;
    for (auto& expression : expressions) {
        if (!excludeSet.contains(expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

logical_type_vec_t ExpressionUtil::getDataTypes(const expression_vector& expressions) {
    std::vector<LogicalType> result;
    result.reserve(expressions.size());
    for (auto& expression : expressions) {
        result.push_back(expression->getDataType().copy());
    }
    return result;
}

expression_vector ExpressionUtil::removeDuplication(const expression_vector& expressions) {
    expression_vector result;
    expression_set expressionSet;
    for (auto& expression : expressions) {
        if (expressionSet.contains(expression)) {
            continue;
        }
        result.push_back(expression);
        expressionSet.insert(expression);
    }
    return result;
}

bool ExpressionUtil::isEmptyPattern(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PATTERN) {
        return false;
    }
    return expression.constCast<NodeOrRelExpression>().isEmpty();
}

bool ExpressionUtil::isNodePattern(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PATTERN) {
        return false;
    }
    return expression.dataType.getLogicalTypeID() == LogicalTypeID::NODE;
};

bool ExpressionUtil::isRelPattern(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PATTERN) {
        return false;
    }
    return expression.dataType.getLogicalTypeID() == LogicalTypeID::REL;
}

bool ExpressionUtil::isRecursiveRelPattern(const Expression& expression) {
    if (expression.expressionType != ExpressionType::PATTERN) {
        return false;
    }
    return expression.dataType.getLogicalTypeID() == LogicalTypeID::RECURSIVE_REL;
}

bool ExpressionUtil::isNullLiteral(const Expression& expression) {
    if (expression.expressionType != ExpressionType::LITERAL) {
        return false;
    }
    return expression.constCast<LiteralExpression>().getValue().isNull();
}

bool ExpressionUtil::isFalseLiteral(const Expression& expression) {
    if (expression.expressionType != ExpressionType::LITERAL) {
        return false;
    }
    return expression.constCast<LiteralExpression>().getValue().getValue<bool>() == false;
}

bool ExpressionUtil::isEmptyList(const kuzu::binder::Expression& expression) {
    if (expression.expressionType != ExpressionType::LITERAL) {
        return false;
    }
    auto val = expression.constCast<LiteralExpression>().getValue();
    if (val.getDataType().getLogicalTypeID() != LogicalTypeID::LIST) {
        return false;
    }
    return val.getChildrenSize() == 0;
}

void ExpressionUtil::validateExpressionType(const Expression& expr,
    common::ExpressionType expectedType) {
    if (expr.expressionType == expectedType) {
        return;
    }
    throw BinderException(stringFormat("{} has type {} but {} was expected.", expr.toString(),
        ExpressionTypeUtil::toString(expr.expressionType),
        ExpressionTypeUtil::toString(expectedType)));
}

void ExpressionUtil::validateExpressionType(const Expression& expr,
    std::vector<common::ExpressionType> expectedType) {
    if (std::find(expectedType.begin(), expectedType.end(), expr.expressionType) !=
        expectedType.end()) {
        return;
    }
    std::string expectedTypesStr = "";
    std::for_each(expectedType.begin(), expectedType.end(),
        [&expectedTypesStr](common::ExpressionType type) {
            expectedTypesStr += expectedTypesStr.empty() ? ExpressionTypeUtil::toString(type) :
                                                           "," + ExpressionTypeUtil::toString(type);
        });
    throw BinderException(stringFormat("{} has type {} but {} was expected.", expr.toString(),
        ExpressionTypeUtil::toString(expr.expressionType), expectedTypesStr));
}

void ExpressionUtil::validateDataType(const Expression& expr, const LogicalType& expectedType) {
    if (expr.getDataType() == expectedType) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), expectedType.toString()));
}

void ExpressionUtil::validateDataType(const Expression& expr, LogicalTypeID expectedTypeID) {
    if (expr.getDataType().getLogicalTypeID() == expectedTypeID) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), LogicalTypeUtils::toString(expectedTypeID)));
}

void ExpressionUtil::validateDataType(const Expression& expr,
    const std::vector<LogicalTypeID>& expectedTypeIDs) {
    auto targetsSet =
        std::unordered_set<LogicalTypeID>{expectedTypeIDs.begin(), expectedTypeIDs.end()};
    if (targetsSet.contains(expr.getDataType().getLogicalTypeID())) {
        return;
    }
    throw BinderException(stringFormat("{} has data type {} but {} was expected.", expr.toString(),
        expr.getDataType().toString(), LogicalTypeUtils::toString(expectedTypeIDs)));
}

template<>
uint64_t ExpressionUtil::getLiteralValue(const Expression& expr) {
    validateExpressionType(expr, ExpressionType::LITERAL);
    validateDataType(expr, LogicalType::UINT64());
    return expr.constCast<LiteralExpression>().getValue().getValue<uint64_t>();
}
template<>
int64_t ExpressionUtil::getLiteralValue(const Expression& expr) {
    validateExpressionType(expr, ExpressionType::LITERAL);
    validateDataType(expr, LogicalType::INT64());
    return expr.constCast<LiteralExpression>().getValue().getValue<int64_t>();
}
template<>
bool ExpressionUtil::getLiteralValue(const Expression& expr) {
    validateExpressionType(expr, ExpressionType::LITERAL);
    validateDataType(expr, LogicalType::BOOL());
    return expr.constCast<LiteralExpression>().getValue().getValue<bool>();
}
template<>
std::string ExpressionUtil::getLiteralValue(const Expression& expr) {
    validateExpressionType(expr, ExpressionType::LITERAL);
    validateDataType(expr, LogicalType::STRING());
    return expr.constCast<LiteralExpression>().getValue().getValue<std::string>();
}
template<>
double ExpressionUtil::getLiteralValue(const Expression& expr) {
    validateExpressionType(expr, ExpressionType::LITERAL);
    validateDataType(expr, LogicalType::DOUBLE());
    return expr.constCast<LiteralExpression>().getValue().getValue<double>();
}

// For primitive types, two types are compatible if they have the same id.
// For nested types, two types are compatible if they have the same id and their children are also
// compatible.
// E.g. [NULL] is compatible with [1,2]
// E.g. {a: NULL, b: NULL} is compatible with {a: [1,2], b: ['c']}
static bool compatible(const LogicalType& type, const LogicalType& target) {
    if (type.getLogicalTypeID() == LogicalTypeID::ANY) {
        return true;
    }
    if (type.getLogicalTypeID() != target.getLogicalTypeID()) {
        return false;
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        return compatible(ListType::getChildType(type), ListType::getChildType(target));
    }
    case LogicalTypeID::ARRAY: {
        return compatible(ArrayType::getChildType(type), ArrayType::getChildType(target));
    }
    case LogicalTypeID::STRUCT: {
        if (StructType::getNumFields(type) != StructType::getNumFields(target)) {
            return false;
        }
        for (auto i = 0u; i < StructType::getNumFields(type); ++i) {
            if (!compatible(StructType::getField(type, i).getType(),
                    StructType::getField(target, i).getType())) {
                return false;
            }
        }
        return true;
    }
    case LogicalTypeID::DECIMAL:
    case LogicalTypeID::UNION:
    case LogicalTypeID::MAP:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
        return false;
    default:
        return true;
    }
}

// Handle special cases where value can be compatible to a type. This happens when a value is a
// nested value but does not have any child.
// E.g. [] is compatible with [1,2]
static bool compatible(const Value& value, const LogicalType& targetType) {
    if (value.isNull()) { // Value is null. We can safely change its type.
        return true;
    }
    if (value.getDataType().getLogicalTypeID() != targetType.getLogicalTypeID()) {
        return false;
    }
    switch (value.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        if (!value.hasNoneNullChildren()) { // Empty list free to change.
            return true;
        }
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            if (!compatible(*NestedVal::getChildVal(&value, i),
                    ListType::getChildType(targetType))) {
                return false;
            }
        }
        return true;
    }
    case LogicalTypeID::ARRAY: {
        if (!value.hasNoneNullChildren()) { // Empty array free to change.
            return true;
        }
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            if (!compatible(*NestedVal::getChildVal(&value, i),
                    ArrayType::getChildType(targetType))) {
                return false;
            }
        }
        return true;
    }
    case LogicalTypeID::MAP: {
        if (!value.hasNoneNullChildren()) { // Empty map free to change.
            return true;
        }
        const auto& keyType = MapType::getKeyType(targetType);
        const auto& valType = MapType::getValueType(targetType);
        for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
            auto childVal = NestedVal::getChildVal(&value, i);
            KU_ASSERT(NestedVal::getChildrenSize(childVal) == 2);
            auto key = NestedVal::getChildVal(childVal, 0);
            auto val = NestedVal::getChildVal(childVal, 1);
            if (!compatible(*key, keyType) || !compatible(*val, valType)) {
                return false;
            }
        }
        return true;
    }
    default:
        break;
    }
    return compatible(value.getDataType(), targetType);
}

bool ExpressionUtil::tryCombineDataType(const expression_vector& expressions, LogicalType& result) {
    std::vector<Value> secondaryValues;
    std::vector<LogicalType> primaryTypes;
    for (auto& expr : expressions) {
        if (expr->expressionType != common::ExpressionType::LITERAL) {
            primaryTypes.push_back(expr->getDataType().copy());
            continue;
        }
        auto literalExpr = expr->constPtrCast<LiteralExpression>();
        if (literalExpr->getValue().allowTypeChange()) {
            secondaryValues.push_back(literalExpr->getValue());
            continue;
        }
        primaryTypes.push_back(expr->getDataType().copy());
    }
    if (!LogicalTypeUtils::tryGetMaxLogicalType(primaryTypes, result)) {
        return false;
    }
    for (auto& value : secondaryValues) {
        if (compatible(value, result)) {
            continue;
        }
        if (!LogicalTypeUtils::tryGetMaxLogicalType(result, value.getDataType(), result)) {
            return false;
        }
    }
    return true;
}

bool ExpressionUtil::canCastStatically(const Expression& expr, const LogicalType& targetType) {
    switch (expr.expressionType) {
    case ExpressionType::LITERAL: {
        auto value = expr.constPtrCast<LiteralExpression>()->getValue();
        return compatible(value, targetType);
    }
    case ExpressionType::PARAMETER: {
        auto value = expr.constPtrCast<ParameterExpression>()->getValue();
        return compatible(value, targetType);
    }
    default:
        return compatible(expr.getDataType(), targetType);
    }
}

} // namespace binder
} // namespace kuzu
