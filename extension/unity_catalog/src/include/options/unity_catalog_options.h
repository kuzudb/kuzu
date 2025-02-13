#pragma once

#include "common/types/value/value.h"

namespace kuzu {
namespace main {
class Database;
class ClientContext;
} // namespace main

namespace unity_catalog_extension {

struct UnityCatalogToken {
    static constexpr const char* NAME = "uc_token";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{"not-used"}; }
};

struct UnityCatalogEndPoint {
    static constexpr const char* NAME = "uc_endpoint";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static common::Value getDefaultValue() { return common::Value{"http://127.0.0.1:8080"}; }
};

struct UnityCatalogOptions {
    static void registerExtensionOptions(main::Database* db);
    static void setEnvValue(main::ClientContext* context);
};

struct DuckDBUnityCatalogSecretManager {
    static std::string getSecret(main::ClientContext* context);
};

} // namespace unity_catalog_extension
} // namespace kuzu
