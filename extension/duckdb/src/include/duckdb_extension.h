#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBExtension final : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

} // namespace duckdb_extension
} // namespace kuzu
