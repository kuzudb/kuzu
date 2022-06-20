#include "src/common/include/vector/value_vector.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {

ValueVector::ValueVector(MemoryManager* memoryManager, DataType dataType)
    : dataType{move(dataType)} {
    assert(memoryManager != nullptr);
    valueBuffer =
        make_unique<uint8_t[]>(Types::getDataTypeSize(dataType) * DEFAULT_VECTOR_CAPACITY);
    values = valueBuffer.get();
    if (needOverflowBuffer()) {
        overflowBuffer = make_unique<OverflowBuffer>(memoryManager);
    }
    nullMask = make_shared<NullMask>();
    numBytesPerValue = Types::getDataTypeSize(dataType);
}

void ValueVector::addString(uint64_t pos, char* value, uint64_t len) const {
    assert(dataType.typeID == STRING);
    auto vectorData = (gf_string_t*)values;
    auto& result = vectorData[pos];
    OverflowBufferUtils::copyString(value, len, result, *overflowBuffer);
}

void ValueVector::addString(uint64_t pos, string value) const {
    addString(pos, value.data(), value.length());
}

} // namespace common
} // namespace graphflow
