#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace delta_extension {

class DeltaExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "DELTA";

public:
    static void load(main::ClientContext* context);
};

} // namespace delta_extension
} // namespace kuzu
