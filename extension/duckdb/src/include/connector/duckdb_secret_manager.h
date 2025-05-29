#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace httpfs_extension {
struct S3FileSystemConfig;
}

namespace duckdb_extension {

struct DuckDBSecretManager {
    static std::string getRemoteS3FSSecret(main::ClientContext* context,
        const httpfs_extension::S3FileSystemConfig& config);
};

} // namespace duckdb_extension
} // namespace kuzu
