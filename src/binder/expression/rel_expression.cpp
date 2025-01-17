#include "binder/expression/rel_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::string RelExpression::detailsToString() const {
    std::string result = toString();
    switch (relType) {
    case QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    case QueryRelType::WEIGHTED_SHORTEST: {
        result += "WEIGHTED SHORTEST";
    }
    default:
        break;
    }
    if (QueryRelTypeUtils::isRecursive(relType)) {
        result += std::to_string(getLowerBound());
        result += "..";
        result += std::to_string(getUpperBound());
    }
    return result;
}

} // namespace binder
} // namespace kuzu