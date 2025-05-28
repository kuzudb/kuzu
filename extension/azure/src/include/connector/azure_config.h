#pragma once

#include <string>

#include "main/database.h"
#include "main/client_context.h"

namespace kuzu {
namespace azure_extension {

struct AzureConfig {
    std::string connectionString;
    static AzureConfig getDefault();
    void registerExtensionOptions(main::Database* db) const;
    void initFromEnv(main::ClientContext* context) const;
};


} // namespace azure_extension
} // namespace kuzu
