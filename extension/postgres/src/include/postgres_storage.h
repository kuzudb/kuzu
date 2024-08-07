#pragma once

#include "storage/storage_extension.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace postgres_extension {

class PostgresStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* DB_TYPE = "POSTGRES";

    static constexpr const char* DEFAULT_SCHEMA_NAME = "public";

    explicit PostgresStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace postgres_extension
} // namespace kuzu
