#pragma once

#include "extension/extension.h"
#include "main/database.h"

namespace kuzu {
namespace duckdb_scanner {

class DuckDBScannerExtension final : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

} // namespace duckdb_scanner
} // namespace kuzu
