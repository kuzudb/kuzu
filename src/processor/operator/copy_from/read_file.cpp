#include "processor/operator/copy_from/read_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ReadFile::getNextTuplesInternal(ExecutionContext* context) {
    auto nodeOffsetVector = resultSet->getValueVector(nodeOffsetPos).get();
    auto morsel = preservingOrder ? sharedState->getMorselSerial() : sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    nodeOffsetVector->setValue(
        nodeOffsetVector->state->selVector->selectedPositions[0], morsel->rowsRead);
    auto recordTable = readTuples(std::move(morsel));
    for (auto i = 0u; i < dataColumnPoses.size(); i++) {
        ArrowColumnVector::setArrowColumn(
            resultSet->getValueVector(dataColumnPoses[i]).get(), recordTable->column((int)i));
    }
    resultSet->dataChunks[0]->state->setToUnflat();
    return true;
}

} // namespace processor
} // namespace kuzu
