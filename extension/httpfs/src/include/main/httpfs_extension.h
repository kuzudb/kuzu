#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace httpfs_extension {

class HttpfsExtension : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "HTTPFS";

public:
    static void load(main::ClientContext* context);
};

} // namespace httpfs_extension
} // namespace kuzu
