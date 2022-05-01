#include "src/storage/include/storage_structure/lists/list_headers.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

ListHeaders::ListHeaders(uint32_t size) : size{size} {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    headers = make_unique<uint32_t[]>(size);
}

ListHeaders::ListHeaders(const string& listBaseFName) : ListHeaders(0) {
    readFromDisk(listBaseFName);
    logger->trace("AdjListHeaders: #Headers {}", sizeof(headers.get()));
};

void ListHeaders::saveToDisk(const string& fName) {
    StorageUtils::saveListOfIntsToFile(fName + ".headers", (uint8_t*)headers.get(), size);
}

void ListHeaders::readFromDisk(const string& fName) {
    auto listSize = StorageUtils::readListOfIntsFromFile(headers, fName + ".headers");
    this->size = listSize;
}

} // namespace storage
} // namespace graphflow
