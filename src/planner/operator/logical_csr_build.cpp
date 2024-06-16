#include "planner/operator/logical_csr_build.h"
#include "common/exception/not_implemented.h"

namespace kuzu {
namespace planner {

static std::string relToString(const binder::RelExpression& rel) {
    auto result = rel.toString();
    switch (rel.getRelType()) {
    case common::QueryRelType::SHORTEST: {
        result += "SHORTEST";
    } break;
    case common::QueryRelType::ALL_SHORTEST: {
        result += "ALL SHORTEST";
    } break;
    default:
        break;
    }
    if (common::QueryRelTypeUtils::isRecursive(rel.getRelType())) {
        result += std::to_string(rel.getLowerBound());
        result += "..";
        result += std::to_string(rel.getUpperBound());
    }
    return result;
}

f_group_pos_set LogicalCSRBuild::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalID());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalCSRBuild::computeFlatSchema() {
    copyChildSchema(0);
}

void LogicalCSRBuild::computeFactorizedSchema() {
    copyChildSchema(0);
}

std::string LogicalCSRBuild::getExpressionsForPrinting() const {
    auto result = boundNode->toString();
    switch (direction) {
    case ExtendDirection::FWD: {
        result += "-";
        result += relToString(*rel);
        result += "->";
    } break;
    case ExtendDirection::BWD: {
        result += "<-";
        result += relToString(*rel);
        result += "-";
    } break;
    case ExtendDirection::BOTH: {
        result += "<-";
        result += relToString(*rel);
        result += "->";
    } break;
    default:
        throw common::NotImplementedException("BaseLogicalExtend::getExpressionsForPrinting");
    }
    result += nbrNode->toString();
    return result;
}

} // namespace planner
} // namespace kuzu
