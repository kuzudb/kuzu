#include "src/storage/storage_structure/include/lists/unstructured_property_lists_utils.h"

namespace kuzu {
namespace storage {

void UnstrPropListWrapper::increaseCapacityIfNeeded(uint64_t requiredCapacity) {
    if (requiredCapacity < capacity) {
        return;
    }
    uint64_t newCapacity =
        std::max(requiredCapacity, (uint64_t)(capacity * StorageConfig::ARRAY_RESIZING_FACTOR));
    std::unique_ptr<uint8_t[]> newData = std::make_unique<uint8_t[]>(newCapacity);
    memcpy(newData.get(), data.get(), capacity);
    data.reset();
    data = std::move(newData);
    capacity = newCapacity;
}

void UnstrPropListWrapper::removePropertyAtOffset(uint64_t offset, uint64_t dataTypeSize) {
    uint64_t sizeToSlide = StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize;
    uint64_t endOffset = offset + StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize;
    memcpy(data.get() + offset, data.get() + endOffset, size - endOffset);
    size -= sizeToSlide;
}

bool UnstrPropListUtils::findKeyPropertyAndPerformOp(UnstrPropListWrapper* updatedList,
    uint32_t propertyKey, std::function<void(UnstrPropListIterator&)> opToPerform) {
    UnstrPropListIterator itr(updatedList);
    while (itr.hasNext()) {
        auto propKeyDataType = itr.readNextPropKeyValue();
        if (propertyKey == propKeyDataType.keyIdx) {
            opToPerform(itr);
            return true;
        } else {
            itr.skipValue();
        }
    }
    return false;
}

UnstructuredPropertyKeyDataType& UnstrPropListIterator::readNextPropKeyValue() {
    memcpy(reinterpret_cast<uint8_t*>(&propKeyDataTypeForRetVal),
        unstrPropListWrapper->data.get() + curOff, StorageConfig::UNSTR_PROP_HEADER_LEN);
    curOff += StorageConfig::UNSTR_PROP_HEADER_LEN;
    return propKeyDataTypeForRetVal;
}

void UnstrPropListIterator::skipValue() {
    curOff += Types::getDataTypeSize(propKeyDataTypeForRetVal.dataTypeID);
    propKeyDataTypeForRetVal.keyIdx = UINT32_MAX;
}

} // namespace storage
} // namespace kuzu
