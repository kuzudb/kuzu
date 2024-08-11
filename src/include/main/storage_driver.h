#pragma once

#include "database.h"

namespace kuzu {
namespace storage {
class Table;
}

namespace main {

class ClientContext;
class KUZU_API StorageDriver {
public:
    explicit StorageDriver(Database* database);

    ~StorageDriver();

    void scan(const std::string& nodeName, const std::string& propertyName,
        common::offset_t* offsets, size_t numOffsets, uint8_t* result, size_t numThreads);

    uint64_t getNumNodes(const std::string& nodeName);
    uint64_t getNumRels(const std::string& relName);

private:
    void scanColumn(storage::Table* table, common::column_id_t columnID, common::offset_t* offsets,
        size_t size, uint8_t* result);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
};

} // namespace main
} // namespace kuzu
