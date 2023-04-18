#include "planner/logical_plan/logical_operator/base_logical_extend.h"

namespace kuzu {
namespace planner {

static std::string relToString(const binder::RelExpression& rel) {
    auto result = rel.toString();
    switch (rel.getRelType()) {
    case common::QueryRelType::SHORTEST:
        result += "SHORTEST";
    case common::QueryRelType::VARIABLE_LENGTH: {
        result += std::to_string(rel.getLowerBound());
        result += "..";
        result += std::to_string(rel.getUpperBound());
    } break;
    default:
        break;
    }
    return result;
}

std::string BaseLogicalExtend::getExpressionsForPrinting() const {
    auto result = boundNode->toString();
    if (direction == common::RelDirection::FWD) {
        result += "-";
        result += relToString(*rel);
        result += "->";
    } else {
        result += "<-";
        result += relToString(*rel);
        result += "-";
    }
    result += nbrNode->toString();
    return result;
}

} // namespace planner
} // namespace kuzu
