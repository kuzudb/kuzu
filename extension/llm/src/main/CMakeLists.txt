add_library(llm_extension_main
        OBJECT
        llm_extension.cpp)

set(LLM_EXTENSION_OBJECT_FILES
        ${LLM_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:llm_extension_main>
        PARENT_SCOPE)
