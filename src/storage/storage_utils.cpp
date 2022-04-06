#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

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

} // namespace storage
} // namespace graphflow
