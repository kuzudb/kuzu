#include "src/storage/include/storage_structure/lists/utils.h"

#include <string>

#include "src/common/include/file_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void saveListOfIntsToFile(const string& fName, unique_ptr<uint32_t[]>& data, uint32_t listSize) {
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    FileUtils::writeToFile(fileInfo.get(), data.get(), sizeof(uint32_t) * listSize, 0 /*offset*/);
    FileUtils::closeFile(fileInfo->fd);
}

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName) {
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
