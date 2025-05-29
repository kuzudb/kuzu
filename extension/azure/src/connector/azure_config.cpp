#include "connector/azure_config.h"

namespace kuzu {
namespace azure_extension {

using namespace common;

AzureConfig AzureConfig::getDefault() {
    return AzureConfig{ .connectionString = "" };
}

void AzureConfig::registerExtensionOptions(main::Database* db) const {
    db->addExtensionOption("AZURE_CONNECTION_STRING", LogicalTypeID::STRING,
        Value::createValue(connectionString), true);
}

void AzureConfig::initFromEnv(main::ClientContext* context) const {
    auto envValue = main::ClientContext::getEnvVariable("AZURE_CONNECTION_STRING");
    if (!envValue.empty()) {
        context->setExtensionOption("AZURE_CONNECTION_STRING", Value::createValue(envValue));
    }
}

} // namespace azure_extension
} // namespace kuzu
