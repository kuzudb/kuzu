#pragma once

#include "main/database.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace sqlite_extension {

class SqliteStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* DB_TYPE = "SQLITE";

    static constexpr const char* DEFAULT_SCHEMA_NAME = "main";

    explicit SqliteStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace sqlite_extension
} // namespace kuzu
