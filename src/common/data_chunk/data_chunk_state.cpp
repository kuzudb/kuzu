#include "common/data_chunk/data_chunk_state.h"

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

std::shared_ptr<DataChunkState> DataChunkState::getSingleValueDataChunkState() {
    auto state = std::make_shared<DataChunkState>(1);
    state->initOriginalAndSelectedSize(1);
    state->setToFlat();
    return state;
}

void DataChunkState::slice(offset_t offset) {
    // NOTE: this operation has performance penalty. Ideally we should directly modify selVector
    // instead of creating a new one.
    auto slicedSelVector = std::make_shared<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < selVector->getSelSize() - offset; i++) {
        slicedSelVector->getMutableBuffer()[i] = selVector->operator[](i + offset);
    }
    slicedSelVector->setToFiltered(selVector->getSelSize() - offset);
    selVector = std::move(slicedSelVector);
}

} // namespace common
} // namespace kuzu
