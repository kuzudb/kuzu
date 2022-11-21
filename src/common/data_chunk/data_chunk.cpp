#include "common/data_chunk/data_chunk.h"

namespace kuzu {
namespace common {

void DataChunk::insert(uint32_t pos, const shared_ptr<ValueVector>& valueVector) {
    valueVector->setState(this->state);
    assert(valueVectors.size() > pos);
    valueVectors[pos] = valueVector;
}

} // namespace common
} // namespace kuzu
