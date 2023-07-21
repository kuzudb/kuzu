#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

void ReadFile::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    rowIdxVector = resultSet->getValueVector(rowIdxVectorPos).get();
    filePathVector = resultSet->getValueVector(filePathVectorPos).get();
    for (auto& arrowColumnPos : dataColumnPoses) {
        dataColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
    }
}

bool ReadFile::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto morsel = sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    rowIdxVector->setValue(rowIdxVector->state->selVector->selectedPositions[0], morsel->rowIdx);
    rowIdxVector->setValue(
        rowIdxVector->state->selVector->selectedPositions[1], morsel->rowIdxInFile);
    filePathVector->resetAuxiliaryBuffer();
    filePathVector->setValue(
        rowIdxVector->state->selVector->selectedPositions[0], morsel->filePath);
    auto recordBatch = readTuples(std::move(morsel));
    for (auto i = 0u; i < dataColumnVectors.size(); i++) {
        common::ArrowColumnVector::setArrowColumn(
            dataColumnVectors[i], recordBatch->column((int)i));
    }
    return true;
}

} // namespace processor
} // namespace kuzu
