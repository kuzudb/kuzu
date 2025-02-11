#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace httpfs {
struct RemoteFSConfig;
}

namespace duckdb_extension {

struct DuckDBSecretManager {
    static std::string getRemoteFSSecret(main::ClientContext* context,
        const httpfs::RemoteFSConfig& config);
};

} // namespace duckdb_extension
} // namespace kuzu
