#pragma once

#include "common/string_utils.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace duckdb_scanner {

class DuckDBStorageExtension : public storage::StorageExtension {
public:
    DuckDBStorageExtension();

    bool canHandleDB(std::string dbType) const override;
};

} // namespace duckdb_scanner
} // namespace kuzu
