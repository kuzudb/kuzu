#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

uint32_t PageUtils::getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
    auto numBytesPerNullEntry = NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
    auto numNullEntries =
        hasNull ? (uint32_t)ceil(
                      (double)DEFAULT_PAGE_SIZE /
                      (double)(((uint64_t)elementSize << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                               numBytesPerNullEntry)) :
                  0;
    return (DEFAULT_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
}

void StorageUtils::saveListOfIntsToFile(const string& fName, uint8_t* data, uint32_t listSize) {
    assert(data);
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    FileUtils::writeToFile(fileInfo.get(), data, sizeof(uint32_t) * listSize, 0 /*offset*/);
    FileUtils::closeFile(fileInfo->fd);
}

uint32_t StorageUtils::readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName) {
    auto fileInfo = FileUtils::openFile(fName, O_RDONLY);
    auto bytesToRead = FileUtils::getFileSize(fileInfo->fd);
    auto listSize = bytesToRead / sizeof(uint32_t);
    data.reset(new uint32_t[listSize]);
    FileUtils::readFromFile(fileInfo.get(), data.get(), bytesToRead, 0 /*offset*/);
    FileUtils::closeFile(fileInfo->fd);
    return listSize;
}

uint64_t StorageUtils::all1sMaskForLeastSignificantBits(uint64_t numBits) {
    assert(numBits <= 64);
    return numBits == 64 ? UINT64_MAX : (1l << numBits) - 1;
}
} // namespace storage
} // namespace graphflow
