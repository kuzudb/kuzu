#include "src/storage/include/data_structure/lists/list_headers.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/storage/include/data_structure/utils.h"

namespace graphflow {
namespace storage {

ListHeaders::ListHeaders() {
    logger = spdlog::get("storage");
}

ListHeaders::ListHeaders(string path) : ListHeaders() {
    readFromDisk(path);
    logger->trace("AdjListHeaders: #Headers {}", sizeof(headers.get()));
};

void ListHeaders::init(uint32_t size) {
    this->size = size;
    headers = make_unique<uint32_t[]>(size);
}

void ListHeaders::saveToDisk(const string& fname) {
    saveListOfIntsToFile(fname + ".headers", headers, size);
}

void ListHeaders::readFromDisk(const string& fname) {
    auto listSize = readListOfIntsFromFile(headers, fname + ".headers");
    this->size = listSize;
}

} // namespace storage
} // namespace graphflow
