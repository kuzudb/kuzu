#pragma once

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/data_chunk/data_chunk_state.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

class FilteringOperator {

public:
    FilteringOperator() {
        prevSelVector = make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        prevSelVector->selectedPositions = nullptr;
    }

protected:
    inline void reInitToRerunSubPlan() {
        prevSelVector->selectedSize = 0;
        prevSelVector->selectedPositions = nullptr;
    }

    void restoreDataChunkSelectorState(const shared_ptr<DataChunk>& dataChunkToSelect);

    void saveDataChunkSelectorState(const shared_ptr<DataChunk>& dataChunkToSelect);

protected:
    unique_ptr<SelectionVector> prevSelVector;
};
} // namespace processor
} // namespace graphflow
