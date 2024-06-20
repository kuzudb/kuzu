#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace json_extension {

class JsonExtension final : public extension::Extension {
public:
    static void load(main::ClientContext* context);
};

} // namespace json_extension
} // namespace kuzu
