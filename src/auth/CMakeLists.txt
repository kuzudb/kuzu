add_library(kuzu_auth
        OBJECT
        auth_manager.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_auth>
        PARENT_SCOPE)
