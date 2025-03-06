#include "common/enums/query_rel_type.h"

#include "common/assert.h"
#include "function/gds/gds_function_collection.h"

using namespace kuzu::function;

namespace kuzu {
namespace common {

PathSemantic QueryRelTypeUtils::getPathSemantic(QueryRelType queryRelType) {
    switch (queryRelType) {
    case QueryRelType::VARIABLE_LENGTH_WALK:
        return PathSemantic::WALK;
    case QueryRelType::VARIABLE_LENGTH_TRAIL:
        return PathSemantic::TRAIL;
    case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
    case QueryRelType::SHORTEST:
    case QueryRelType::ALL_SHORTEST:
    case QueryRelType::WEIGHTED_SHORTEST:
    case QueryRelType::ALL_WEIGHTED_SHORTEST:
        return PathSemantic::ACYCLIC;
    default:
        KU_UNREACHABLE;
    }
}

function::GDSFunction QueryRelTypeUtils::getFunction(QueryRelType type) {
    switch (type) {
    case QueryRelType::VARIABLE_LENGTH_WALK:
    case QueryRelType::VARIABLE_LENGTH_TRAIL:
    case QueryRelType::VARIABLE_LENGTH_ACYCLIC: {
        return VarLenJoinsFunction::getFunction();
    }
    case QueryRelType::SHORTEST: {
        return SingleSPPathsFunction::getFunction();
    }
    case QueryRelType::ALL_SHORTEST: {
        return AllSPPathsFunction::getFunction();
    }
    case QueryRelType::WEIGHTED_SHORTEST: {
        return WeightedSPPathsFunction::getFunction();
    }
    case QueryRelType::ALL_WEIGHTED_SHORTEST: {
        return AllWeightedSPPathsFunction::getFunction();
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
