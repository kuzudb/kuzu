#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace postgres_extension {

class PostgresExtension final : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

} // namespace postgres_extension
} // namespace kuzu
