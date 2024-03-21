#pragma once

#include "common/string_utils.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace postgres_scanner {

class PostgresStorageExtension : public storage::StorageExtension {
public:
    static constexpr char POSTGRES_SCHEMA_NAME[] = "public";

    static constexpr char POSTGRES_CATALOG_NAME_IN_DUCKDB[] = "pg";

    PostgresStorageExtension();

    bool canHandleDB(std::string dbType) const override;
};

} // namespace postgres_scanner
} // namespace kuzu
