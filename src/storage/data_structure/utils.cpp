#include "src/storage/include/data_structure/utils.h"

#include <string>

namespace graphflow {
namespace storage {

void saveListOfIntsToFile(const string& path, unique_ptr<uint32_t[]>& data, uint32_t listSize) {
    if (0 == path.length()) {
        throw invalid_argument("Lists Aux structures: Empty filename");
    }
    uint32_t f = open(path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        throw invalid_argument("Lists Aux structures: Cannot create file: " + path);
    }
    auto bytesToWrite = sizeof(uint32_t) * listSize;
    if (bytesToWrite != write(f, data.get(), bytesToWrite)) {
        throw invalid_argument("Lists Aux structures: Cannot write in file: " + path);
    }
    close(f);
}

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& path) {
    if (0 == path.length()) {
        throw invalid_argument("Lists Aux structures: Empty filename");
    }
    uint32_t f = open(path.c_str(), O_RDONLY);
    auto bytesToRead = lseek(f, 0, SEEK_END);
    auto listSize = bytesToRead / sizeof(uint32_t);
    data.reset(new uint32_t[listSize]);
    lseek(f, 0, SEEK_SET);
    if (bytesToRead != read(f, data.get(), bytesToRead)) {
        throw invalid_argument("Lists Aux structures: Cannot read from file.");
    }
    close(f);
    return listSize;
}

} // namespace storage
} // namespace graphflow
