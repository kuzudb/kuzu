#include "src/storage/include/storage_structure/lists/list_headers.h"

#include <fstream>

#include "src/common/include/utils.h"
#include "src/storage/include/storage_structure/lists/utils.h"

namespace graphflow {
namespace storage {

ListHeaders::ListHeaders() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

ListHeaders::ListHeaders(const string& listBaseFName) : ListHeaders() {
    readFromDisk(listBaseFName);
    logger->trace("AdjListHeaders: #Headers {}", sizeof(headers.get()));
};

void ListHeaders::init(uint32_t size) {
    this->size = size;
    headers = make_unique<uint32_t[]>(size);
}

void ListHeaders::saveToDisk(const string& fName) {
    saveListOfIntsToFile(fName + ".headers", headers, size);
}

void ListHeaders::readFromDisk(const string& fName) {
    auto listSize = readListOfIntsFromFile(headers, fName + ".headers");
    this->size = listSize;
}

} // namespace storage
} // namespace graphflow
