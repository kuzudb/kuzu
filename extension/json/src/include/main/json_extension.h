#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace json_extension {

class JsonExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "JSON";

public:
    static constexpr char JSON_TYPE_NAME[] = "json";
    static constexpr common::idx_t JSON_SCAN_FILE_IDX = 0;

    static void load(main::ClientContext* context);
};

} // namespace json_extension
} // namespace kuzu
