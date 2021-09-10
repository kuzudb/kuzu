#include "src/processor/include/physical_plan/operator/physical_operator_info.h"

namespace graphflow {
namespace processor {

PhysicalOperatorsInfo::PhysicalOperatorsInfo(const Schema& schema) {
    auto dataChunkPos = 0ul;
    for (auto& group : schema.groups) {
        auto vectorPos = 0ul;
        for (auto& variable : group->variables) {
            variableToDataPosMap.insert({variable, make_pair(dataChunkPos, vectorPos)});
            vectorPos++;
        }
        dataChunkPosToNumValueVectors.insert({dataChunkPos, vectorPos});
        dataChunkPos++;
    }
    totalNumDataChunks = dataChunkPos;
}

} // namespace processor
} // namespace graphflow
