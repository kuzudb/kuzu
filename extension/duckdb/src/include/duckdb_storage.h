#pragma once

#include "main/database.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* dbType = "DUCKDB";

    static constexpr const char* skipInvalidTable = "SKIPINVALIDTABLE";

    static constexpr bool skipInvalidTableDefaultVal = false;

    explicit DuckDBStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace duckdb_extension
} // namespace kuzu
