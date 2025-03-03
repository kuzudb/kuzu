#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace hnsw_extension {

class HNSWExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "HNSW";

public:
    static void load(main::ClientContext* context);
};
} // namespace hnsw_extension
} // namespace kuzu
