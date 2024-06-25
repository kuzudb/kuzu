#pragma once

#include <memory>
#include <string>

#include "extension/catalog_extension.h"

namespace kuzu {
namespace storage {
class StorageManager;
} // namespace storage

namespace transaction {
class TransactionManager;
} // namespace transaction

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

protected:
    std::string dbName;
    std::string dbType;
    std::unique_ptr<catalog::Catalog> catalog;
};

class AttachedKuzuDatabase : public AttachedDatabase {
public:
    AttachedKuzuDatabase(std::string dbPath, std::string dbName, std::string dbType,
        ClientContext* clientContext);

    storage::StorageManager* getStorageManager() { return storageManager.get(); }

    transaction::TransactionManager* getTransactionManager() { return transactionManager.get(); }

private:
    void initCatalog(const std::string& path, ClientContext* context);

private:
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
};

} // namespace main
} // namespace kuzu
