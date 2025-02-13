#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace httpfs {
struct S3FileSystemConfig;
}

namespace duckdb_extension {

struct DuckDBSecretManager {
    static std::string getRemoteFSSecret(main::ClientContext* context,
        const httpfs::S3FileSystemConfig& config);
};

} // namespace duckdb_extension
} // namespace kuzu
