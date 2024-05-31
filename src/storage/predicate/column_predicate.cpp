#include "storage/predicate/column_predicate.h"

#include "binder/expression/literal_expression.h"
#include "storage/predicate/constant_predicate.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

ZoneMapCheckResult ColumnPredicateSet::checkZoneMap(const CompressionMetadata& metadata) {
    for (auto& predicate : predicates) {
        if (predicate->checkZoneMap(metadata) == ZoneMapCheckResult::SKIP_SCAN) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    }
    return ZoneMapCheckResult::ALWAYS_SCAN;
}

ColumnPredicateSet::ColumnPredicateSet(const ColumnPredicateSet& other) {
    for (auto& p : other.predicates) {
        predicates.push_back(p->copy());
    }
}

static bool isPropertyConstantPair(const Expression& left, const Expression& right) {
    return left.expressionType == ExpressionType::PROPERTY &&
           right.expressionType == ExpressionType::LITERAL;
}

static std::unique_ptr<ColumnPredicate> tryConvertToConstColumnPredicate(const Expression& property,
    const Expression& predicate) {
    if (isPropertyConstantPair(*predicate.getChild(0), *predicate.getChild(1))) {
        if (property != *predicate.getChild(0)) {
            return nullptr;
        }
        auto value = predicate.getChild(1)->constCast<LiteralExpression>().getValue();
        return std::make_unique<ColumnConstantPredicate>(predicate.expressionType, value);
    } else if (isPropertyConstantPair(*predicate.getChild(1), *predicate.getChild(0))) {
        if (property != *predicate.getChild(1)) {
            return nullptr;
        }
        auto value = predicate.getChild(0)->constCast<LiteralExpression>().getValue();
        auto expressionType =
            ExpressionTypeUtil::reverseComparisonDirection(predicate.expressionType);
        return std::make_unique<ColumnConstantPredicate>(expressionType, value);
    }
    // Not a predicate that runs on this property.
    return nullptr;
}

std::unique_ptr<ColumnPredicate> ColumnPredicateUtil::tryConvert(const Expression& property,
    const Expression& predicate) {
    if (ExpressionTypeUtil::isComparison(predicate.expressionType)) {
        return tryConvertToConstColumnPredicate(property, predicate);
    }
    return nullptr;
}

} // namespace storage
} // namespace kuzu
