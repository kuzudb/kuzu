#pragma once

#include <string>

#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace azure_extension {

struct AzureConfig {
    std::string connectionString;
    std::string accountName;
    std::string httpProxy;
    std::string proxyUserName;
    std::string proxyPassword;
    std::vector<std::pair<std::string, const std::string&>> getFields() const {
        return {
            {"AZURE_CONNECTION_STRING", connectionString},
            {"AZURE_ACCOUNT_NAME", accountName},
            {"AZURE_HTTP_PROXY", httpProxy},
            {"AZURE_PROXY_USER_NAME", proxyUserName},
            {"AZURE_PROXY_PASSWORD", proxyPassword}
        };
    }
    static AzureConfig getDefault();
    void registerExtensionOptions(main::Database* db) const;
    void initFromEnv(main::ClientContext* context) const;
};

} // namespace azure_extension
} // namespace kuzu
