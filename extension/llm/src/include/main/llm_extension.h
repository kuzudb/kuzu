#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace llm_extension {

class LLMExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "LLM";

public:
    static void load(main::ClientContext* context);
};
} // namespace vector_extension
} // namespace kuzu
