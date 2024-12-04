#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

struct DuckDBSecretManager {
    static std::string getS3Secret(main::ClientContext* context);
};

} // namespace duckdb_extension
} // namespace kuzu
