#include "src/storage/include/data_structure/utils.h"

#include <string>

#include "src/common/include/file_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void saveListOfIntsToFile(const string& fName, unique_ptr<uint32_t[]>& data, uint32_t listSize) {
    if (0 == fName.length()) {
        throw invalid_argument("Lists Aux structures: Empty filename");
    }
    auto fd = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    FileUtils::writeToFile(fd, data.get(), sizeof(uint32_t) * listSize, 0 /*offset*/);
    FileUtils::closeFile(fd);
}

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName) {
    if (0 == fName.length()) {
        throw invalid_argument("Lists Aux structures: Empty filename");
    }
    auto fd = FileUtils::openFile(fName, O_RDONLY);
    auto bytesToRead = lseek(fd, 0, SEEK_END);
    auto listSize = bytesToRead / sizeof(uint32_t);
    data.reset(new uint32_t[listSize]);
    lseek(fd, 0, SEEK_SET);
    FileUtils::readFromFile(fd, data.get(), bytesToRead, 0 /*offset*/);
    FileUtils::closeFile(fd);
    return listSize;
}

} // namespace storage
} // namespace graphflow
