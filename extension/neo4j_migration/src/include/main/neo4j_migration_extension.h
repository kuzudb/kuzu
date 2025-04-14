#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace neo4j_migration_extension {

class Neo4jMigrationExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "NEO4J_MIGRATION";

public:
    static void load(main::ClientContext* context);
};

} // namespace neo4j_migration_extension
} // namespace kuzu
