#include "processor/operator/copy_from/read_csv.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {
bool ReadCSV::getNextTuplesInternal(ExecutionContext* context) {
    auto morsel = sharedState->getMorselSerial();
    if (morsel == nullptr) {
        return false;
    }
    auto nodeOffsetVector = resultSet->getValueVector(nodeOffsetPos).get();
    nodeOffsetVector->setValue(
            nodeOffsetVector->state->selVector->selectedPositions[0], morsel->rowsRead);
    auto output = readDataChunk(std::move(morsel));
    resultSet->insert(dataColumnPoses[0].dataChunkPos, output);
    resultSet->dataChunks[0]->state->setToUnflat();
    return true;
}

} // namespace processor
} // namespace kuzu
