#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace gds_extension {

class GdsExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "GDS";

public:
    static void load(main::ClientContext* context);
};

} // namespace gds_extension
} // namespace kuzu
