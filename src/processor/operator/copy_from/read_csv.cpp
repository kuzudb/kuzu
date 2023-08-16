#include "processor/operator/copy_from/read_csv.h"
#include <iostream>

using namespace kuzu::storage;

namespace kuzu {
namespace processor {
bool ReadCSV::getNextTuplesInternal(ExecutionContext* context) {
    auto nodeOffsetVector = resultSet->getValueVector(nodeOffsetPos).get();
    auto morsel = sharedState->getMorselSerial();
    if (morsel == nullptr) {
        return false;
    }
    nodeOffsetVector->setValue(
            nodeOffsetVector->state->selVector->selectedPositions[0], morsel->rowsRead);
    auto output = readDataChunk(std::move(morsel));
    //for (auto i = 0u; i < dataColumnPoses.size(); i++) {
    //    std::cout << "HERE8" << std::endl;
    //    resultSet->insert(dataColumnPoses[i].dataChunkPos, output);
    //    std::cout << "HERE9" << std::endl;
    //}
    resultSet->insert(dataColumnPoses[0].dataChunkPos, output);
    resultSet->dataChunks[0]->state->setToUnflat();
    return true;
}

} // namespace processor
} // namespace kuzu
