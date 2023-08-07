#include "planner/logical_plan/extend/logical_extend.h"

#include "planner/logical_plan/factorization/flatten_resolver.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalExtend::getGroupsPosToFlatten() {
    f_group_pos_set result;
    if (hasAtMostOneNbr) { // Column extend. No need to flatten input bound node.
        return result;
    }
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalIDProperty());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalExtend::computeFactorizedSchema() {
    copyChildSchema(0);
    auto boundGroupPos = schema->getGroupPos(boundNode->getInternalIDPropertyName());
    uint32_t nbrGroupPos = 0u;
    if (hasAtMostOneNbr) {
        nbrGroupPos = boundGroupPos;
    } else {
        assert(schema->getGroup(boundGroupPos)->isFlat());
        nbrGroupPos = schema->createGroup();
    }
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, nbrGroupPos);
    }
}

void LogicalExtend::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
}

} // namespace planner
} // namespace kuzu
