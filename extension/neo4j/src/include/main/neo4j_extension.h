#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace neo4j_extension {

class Neo4jExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "NEO4J";

public:
    static void load(main::ClientContext* context);
};

} // namespace neo4j_extension
} // namespace kuzu
