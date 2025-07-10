#include "main/llm_extension.h"

#include "function/llm_functions.h"
#include "main/client_context.h"

namespace kuzu {
namespace llm_extension {

// This extension provides an interface to retrieve text embeddings from supported LLM providers.
// The function defined in create_embedding.cpp accepts ('prompt', 'provider', 'model', {optional
// params}). To add a new provider and its models, implement a class derived from the
// EmbeddingProvider abstract base class. The implemented methods are used in create_embedding.cpp
// to construct requests and parse returned embeddings. Note: Remember to update the provider lookup
// table (static EmbeddingProvider& getInstance(const std::string& provider)) to include the new
// provider.

void LlmExtension::load(main::ClientContext* context) {
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
    kuzu::llm_extension::LlmExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::llm_extension::LlmExtension::EXTENSION_NAME;
}
}
#endif
