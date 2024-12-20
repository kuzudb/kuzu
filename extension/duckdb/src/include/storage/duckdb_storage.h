#pragma once

#include "main/database.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* DB_TYPE = "DUCKDB";

    static constexpr const char* DEFAULT_SCHEMA_NAME = "main";

    static constexpr const char* SKIP_UNSUPPORTED_TABLE_KEY = "SKIP_UNSUPPORTED_TABLE";

    static constexpr const char* SCHEMA_OPTION = "SCHEMA";

    static constexpr bool SKIP_UNSUPPORTED_TABLE_DEFAULT_VAL = false;

    explicit DuckDBStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace duckdb_extension
} // namespace kuzu
