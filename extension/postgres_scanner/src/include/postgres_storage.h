#pragma once

#include "common/string_utils.h"
#include "storage/storage_extension.h"

namespace kuzu {
namespace postgres_scanner {

class PostgresStorageExtension final : public storage::StorageExtension {
public:
    PostgresStorageExtension();

    bool canHandleDB(std::string dbType) const override;
};

} // namespace postgres_scanner
} // namespace kuzu
