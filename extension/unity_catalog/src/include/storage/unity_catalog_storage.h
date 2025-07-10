#pragma once

#include "storage/storage_extension.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace unity_catalog_extension {

class UnityCatalogStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* DB_TYPE = "UC_CATALOG";

    static constexpr const char* DEFAULT_SCHEMA_NAME = "default";

    explicit UnityCatalogStorageExtension(main::Database& database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace unity_catalog_extension
} // namespace kuzu
