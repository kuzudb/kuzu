#include "connector/azure_config.h"

namespace kuzu {
namespace azure_extension {

using namespace common;

AzureConfig AzureConfig::getDefault() {
    return AzureConfig{};
}

void AzureConfig::registerExtensionOptions(main::Database* db) const {
    for (auto [fieldName, field] : getFields()) {
        db->addExtensionOption(fieldName, LogicalTypeID::STRING, Value::createValue(field), true);
    }
}

void AzureConfig::initFromEnv(main::ClientContext* context) const {
    for (auto [fieldName, _] : getFields()) {
        auto envValue = main::ClientContext::getEnvVariable(fieldName);
        if (!envValue.empty()) {
            context->setExtensionOption(fieldName, Value::createValue(envValue));
        }
    }
}

} // namespace azure_extension
} // namespace kuzu
