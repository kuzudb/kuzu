#pragma once

#include "storage/storage_extension.h"

namespace kuzu {
namespace postgres_scanner {

class PostgresStorageExtension final : public storage::StorageExtension {
public:
    static constexpr const char* dbType = "POSTGRES";

    explicit PostgresStorageExtension(main::Database* database);

    bool canHandleDB(std::string dbType) const override;
};

} // namespace postgres_scanner
} // namespace kuzu
