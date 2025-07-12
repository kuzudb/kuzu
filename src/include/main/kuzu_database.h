#pragma once

#include <memory>
#include <string>

#include "database_instance.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace storage {
class StorageManager;
} // namespace storage

namespace main {

class KuzuDatabase final : public DatabaseInstance {
public:
    KuzuDatabase(const std::string& dbPath, std::string dbName, std::string dbType,
        ClientContext* context);

    storage::StorageManager* getStorageManager() const { return storageManager.get(); }

    static void recover(ClientContext& context);

private:
    std::unique_ptr<storage::StorageManager> storageManager;
};

} // namespace main
} // namespace kuzu
