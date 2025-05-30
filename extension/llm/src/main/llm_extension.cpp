#include "main/llm_extension.h"

#include "function/llm_functions.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace llm_extension {

void LLMExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();

    extension::ExtensionUtils::addScalarFunc<CreateEmbedding>(db);
}

} // namespace llm_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::llm_extension::LLMExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::llm_extension::LLMExtension::EXTENSION_NAME;
}
}
#endif
