#include "src/common/include/vector/value_vector.h"

#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {

NullMask::NullMask() : mayContainNulls{false} {
    mask = make_unique<bool[]>(DEFAULT_VECTOR_CAPACITY);
    fill_n(mask.get(), DEFAULT_VECTOR_CAPACITY, false /* not null */);
}

ValueVector::ValueVector(MemoryManager* memoryManager, DataType dataType) : dataType{dataType} {
    assert(memoryManager != nullptr);
    values = make_unique<uint8_t[]>(Types::getDataTypeSize(dataType) * DEFAULT_VECTOR_CAPACITY);
    if (needOverflowBuffer()) {
        overflowBuffer = make_unique<OverflowBuffer>(memoryManager);
    }
    nullMask = make_shared<NullMask>();
}

void ValueVector::addString(uint64_t pos, char* value, uint64_t len) const {
    assert(dataType == STRING);
    auto vectorData = (gf_string_t*)values.get();
    auto& result = vectorData[pos];
    TypeUtils::copyString(value, len, result, *overflowBuffer);
}

void ValueVector::addString(uint64_t pos, string value) const {
    addString(pos, value.data(), value.length());
}

} // namespace common
} // namespace graphflow
