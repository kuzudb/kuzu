#include "main/storage_driver.h"

#include "catalog/catalog.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

StorageDriver::StorageDriver(kuzu::main::Database* database)
    : catalog{database->catalog.get()}, storageManager{database->storageManager.get()} {}

StorageDriver::~StorageDriver() = default;

std::pair<std::unique_ptr<uint8_t[]>, size_t> StorageDriver::scan(const std::string& nodeName,
    const std::string& propertyName, common::offset_t* offsets, size_t size) {
    auto catalogContent = catalog->getReadOnlyVersion();
    auto nodeTableID = catalogContent->getTableID(nodeName);
    auto propertyID = catalogContent->getTableSchema(nodeTableID)->getPropertyID(propertyName);
    auto nodeTable = storageManager->getNodesStore().getNodeTable(nodeTableID);
    auto column = nodeTable->getPropertyColumn(propertyID);

    auto bufferSize = column->elementSize * size;
    auto result = std::make_unique<uint8_t[]>(bufferSize);
    column->scan(offsets, size, result.get());
    return std::make_pair(std::move(result), bufferSize);
}

} // namespace main
} // namespace kuzu
