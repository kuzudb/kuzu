#include "common/data_chunk/data_chunk.h"

namespace kuzu {
namespace common {

void DataChunk::insert(uint32_t pos, std::shared_ptr<ValueVector> valueVector) {
    valueVector->setState(state);
    assert(valueVectors.size() > pos);
    valueVectors[pos] = std::move(valueVector);
}

} // namespace common
} // namespace kuzu
