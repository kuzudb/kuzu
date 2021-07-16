#include "src/storage/include/data_structure/lists/utils.h"

#include <string>

#include "src/common/include/file_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void saveListOfIntsToFile(const string& fName, unique_ptr<uint32_t[]>& data, uint32_t listSize) {
    auto fd = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    FileUtils::writeToFile(fd, data.get(), sizeof(uint32_t) * listSize, 0 /*offset*/);
    FileUtils::closeFile(fd);
}

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName) {
    auto fd = FileUtils::openFile(fName, O_RDONLY);
    auto bytesToRead = FileUtils::getFileSize(fd);
    auto listSize = bytesToRead / sizeof(uint32_t);
    data.reset(new uint32_t[listSize]);
    FileUtils::readFromFile(fd, data.get(), bytesToRead, 0 /*offset*/);
    FileUtils::closeFile(fd);
    return listSize;
}

} // namespace storage
} // namespace graphflow
