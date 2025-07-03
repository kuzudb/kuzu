add_library(kuzu_embedding_providers
        OBJECT
        open-ai.cpp
        ollama.cpp
        voyage-ai.cpp
        google-vertex.cpp
        google-gemini.cpp
        amazon-bedrock.cpp
)

set(embedding_EXTENSION_OBJECT_FILES
        ${embedding_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_embedding_providers>
        PARENT_SCOPE)
