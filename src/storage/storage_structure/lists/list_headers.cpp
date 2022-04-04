#include "src/storage/include/storage_structure/lists/list_headers.h"

#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

ListHeaders::ListHeaders() : size{0} {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

ListHeaders::ListHeaders(const string& listBaseFName) : ListHeaders() {
    readFromDisk(listBaseFName);
    logger->trace("AdjListHeaders: #Headers {}", sizeof(headers.get()));
};

void ListHeaders::init(uint32_t size_) {
    this->size = size_;
    headers = make_unique<uint32_t[]>(size_);
}

void ListHeaders::saveToDisk(const string& fName) {
    StorageUtils::saveListOfIntsToFile(fName + ".headers", headers, size);
}

void ListHeaders::readFromDisk(const string& fName) {
    auto listSize = StorageUtils::readListOfIntsFromFile(headers, fName + ".headers");
    this->size = listSize;
}

} // namespace storage
} // namespace graphflow
