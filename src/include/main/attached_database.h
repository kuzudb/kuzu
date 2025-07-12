#pragma once

#include <memory>
#include <string>

#include "extension/catalog_extension.h"
#include "transaction/transaction_manager.h"

namespace duckdb {
class MaterializedQueryResult;
}

namespace kuzu {
namespace storage {
class StorageManager;
} // namespace storage

namespace main {

class AttachedDatabase {
public:
    AttachedDatabase(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog)
        : dbName{std::move(dbName)}, dbType{std::move(dbType)}, catalog{std::move(catalog)} {}

    virtual ~AttachedDatabase() = default;

    std::string getDBName() const { return dbName; }

    std::string getDBType() const { return dbType; }

    catalog::Catalog* getCatalog() { return catalog.get(); }

    void invalidateCache();

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

protected:
    std::string dbName;
    std::string dbType;
    std::unique_ptr<catalog::Catalog> catalog;
};

class AttachedKuzuDatabase final : public AttachedDatabase {
public:
    AttachedKuzuDatabase(const std::string& dbPath, std::string dbName, std::string dbType,
        ClientContext* clientContext);

    storage::StorageManager* getStorageManager() { return storageManager.get(); }

private:
    std::unique_ptr<storage::StorageManager> storageManager;
};

} // namespace main
} // namespace kuzu
