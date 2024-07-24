#include "binder/expression/rel_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::string RelExpression::detailsToString() const {
    std::string result = toString();
    switch (relType) {
    case common::QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case common::QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    default:
        break;
    }
    if (common::QueryRelTypeUtils::isRecursive(relType)) {
        result += std::to_string(getLowerBound());
        result += "..";
        result += std::to_string(getUpperBound());
    }
    return result;
}

} // namespace binder
} // namespace kuzu