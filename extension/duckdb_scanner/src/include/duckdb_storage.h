#pragma once

#include "main/database.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace duckdb_scanner {

class DuckDBStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* dbType = "DUCKDB";

    explicit DuckDBStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace duckdb_scanner
} // namespace kuzu
