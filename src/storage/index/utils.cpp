#include "src/storage/include/index/utils.h"

#include <cstring>

#include "src/common/include/operations/hash_operations.h"

using namespace std;

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataType keyDataType) : keyDataType{keyDataType} {
    numBytesPerEntry = TypeUtils::getDataTypeSize(keyDataType) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * slotCapacity) + sizeof(SlotHeader);
    numSlotsPerPage = PAGE_SIZE / numBytesPerSlot;
}

void HashIndexHeader::incrementLevel() {
    currentLevel++;
    levelHashMask = (1 << currentLevel) - 1;
    higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
}

uint64_t HashIndexUtils::calculateSlotIdForHash(const HashIndexHeader& header, hash_t hash) {
    auto slotId = hash & header.levelHashMask;
    slotId = slotId >= header.nextSplitSlotId ? slotId : (hash & header.higherLevelHashMask);
    return slotId;
}

uint8_t* HashIndexUtils::getNewPage() {
    auto newPage = new uint8_t[PAGE_SIZE];
    memset(newPage, 0, PAGE_SIZE);
    return newPage;
}

hash_t HashIndexUtils::hashFunc(const HashIndexHeader& header, uint8_t* key) {
    hash_t hash;
    switch (header.keyDataType) {
    case INT64:
        operation::Hash::operation(*(int64_t*)(key), false /*isNULL*/, hash);
        break;
    case STRING:
        hash = std::hash<string>{}(reinterpret_cast<const char*>(key));
        break;
    default:
        throw invalid_argument(
            "Type " + TypeUtils::dataTypeToString(header.keyDataType) + " not supported.");
    }
    return hash;
}

bool HashIndexUtils::equalsInt64(uint8_t* key, uint8_t* entryKey) {
    return memcmp(key, entryKey, sizeof(int64_t)) == 0;
}

bool HashIndexUtils::likelyEqualsString(uint8_t* key, gf_string_t* entryKey) {
    if (memcmp(key, entryKey->prefix, gf_string_t::PREFIX_LENGTH) != 0) {
        return false;
    }
    if (strlen(reinterpret_cast<const char*>(key)) != entryKey->len) {
        return false;
    }
    return true;
}

} // namespace storage
} // namespace graphflow