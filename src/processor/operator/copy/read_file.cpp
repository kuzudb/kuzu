#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

void ReadFile::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    rowIdxVector = resultSet->getValueVector(rowIdxVectorPos).get();
    for (auto& arrowColumnPos : arrowColumnPoses) {
        arrowColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
    }
}

bool ReadFile::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto morsel = sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    rowIdxVector->setValue(
        rowIdxVector->state->selVector->selectedPositions[0], morsel->rowIdxInFile);
    auto recordBatch = readTuples(std::move(morsel));
    for (auto i = 0u; i < arrowColumnPoses.size(); i++) {
        common::ArrowColumnVector::setArrowColumn(
            resultSet->getValueVector(arrowColumnPoses[i]).get(), recordBatch->column((int)i));
    }
    resultSet->dataChunks[0]->state->currIdx = -1;
    return true;
}

} // namespace processor
} // namespace kuzu
