#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace embedding_extension {

class embeddingExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "EMBEDDING";

public:
    static void load(main::ClientContext* context);
};
} // namespace embedding_extension
} // namespace kuzu
