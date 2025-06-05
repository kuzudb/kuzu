add_library(kuzu_llm_providers
        OBJECT
        open-ai.cpp)

set(LLM_EXTENSION_OBJECT_FILES
        ${LLM_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_llm_providers>
        PARENT_SCOPE)
