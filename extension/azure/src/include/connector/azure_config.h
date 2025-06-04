#pragma once

#include <string>

#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace azure_extension {

struct AzureConfig {
    std::string connectionString;
    std::string accountName;
    std::vector<std::pair<std::string, const std::string&>> getFields() const {
        return {{"AZURE_CONNECTION_STRING", connectionString}, {"AZURE_ACCOUNT_NAME", accountName}};
    }
    static AzureConfig getDefault();
    void registerExtensionOptions(main::Database* db) const;
    void initFromEnv(main::ClientContext* context) const;
};

} // namespace azure_extension
} // namespace kuzu
