#include "processor/operator/base_shortest_path.h"

#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

void BaseShortestPath::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    srcValueVector =
        resultSet->dataChunks[srcDataPos.dataChunkPos]->getValueVector(srcDataPos.valueVectorPos);
    destValueVector =
        resultSet->dataChunks[destDataPos.dataChunkPos]->getValueVector(destDataPos.valueVectorPos);
}

} // namespace processor
} // namespace kuzu
