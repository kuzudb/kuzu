#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

void ReadFile::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    offsetVector = resultSet->getValueVector(offsetVectorPos).get();
    for (auto& arrowColumnPos : arrowColumnPoses) {
        arrowColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
    }
}

bool ReadFile::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto morsel = sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    auto startOffset = morsel->nodeOffset;
    offsetVector->setValue(offsetVector->state->selVector->selectedPositions[0], startOffset);
    auto recordBatch = readTuples(std::move(morsel));
    for (auto i = 0u; i < arrowColumnVectors.size(); i++) {
        common::ArrowColumnVector::setArrowColumn(
            arrowColumnVectors[i], recordBatch->column((int)i));
    }
    return true;
}

} // namespace processor
} // namespace kuzu
