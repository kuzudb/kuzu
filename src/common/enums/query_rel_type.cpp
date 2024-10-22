#include "common/enums/query_rel_type.h"

#include "common/assert.h"

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
        return PathSemantic::ACYCLIC;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
