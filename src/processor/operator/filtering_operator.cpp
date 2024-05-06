#include "processor/operator/filtering_operator.h"

#include <cstring>

#include "common/data_chunk/data_chunk_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void SelVectorOverWriter::restoreSelVector(DataChunkState& dataChunkState) {
    if (prevSelVector != nullptr) {
        dataChunkState.setSelVector(prevSelVector);
    }
}

void SelVectorOverWriter::saveSelVector(DataChunkState& dataChunkState) {
    if (prevSelVector == nullptr) {
        prevSelVector = dataChunkState.getSelVectorShared();
    }
    resetCurrentSelVector(dataChunkState.getSelVector());
    dataChunkState.setSelVector(currentSelVector);
}

void SelVectorOverWriter::resetCurrentSelVector(const SelectionVector& selVector) {
    currentSelVector->setSelSize(selVector.getSelSize());
    if (selVector.isUnfiltered()) {
        currentSelVector->setToUnfiltered();
    } else {
        std::memcpy(currentSelVector->getMultableBuffer().data(),
            selVector.getSelectedPositions().data(), selVector.getSelSize() * sizeof(sel_t));
        currentSelVector->setToFiltered();
    }
}

} // namespace processor
} // namespace kuzu
