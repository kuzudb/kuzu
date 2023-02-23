#pragma once

#include "common/types/types_include.h"
#include "database.h"

namespace kuzu {
namespace main {

class StorageDriver {
public:
    explicit StorageDriver(Database* database);

    ~StorageDriver();

    std::pair<std::unique_ptr<uint8_t[]>, size_t> scan(const std::string& nodeName,
        const std::string& propertyName, common::offset_t* offsets, size_t size);

private:
    catalog::Catalog* catalog;
    storage::StorageManager* storageManager;
};

} // namespace main
} // namespace kuzu
