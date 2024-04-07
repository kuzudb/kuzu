#pragma once

#include "extension/extension.h"
#include "main/database.h"

namespace kuzu {
namespace postgres_scanner {

class PostgresScannerExtension final : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

} // namespace postgres_scanner
} // namespace kuzu
