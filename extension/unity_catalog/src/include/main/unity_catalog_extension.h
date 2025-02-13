#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace unity_catalog_extension {

class UnityCatalogExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "UNITY_CATALOG";

public:
    static void load(main::ClientContext* context);
};

} // namespace unity_catalog_extension
} // namespace kuzu
