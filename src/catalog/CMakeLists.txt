add_library(kuzu_catalog
        OBJECT
        catalog.cpp
        catalog_structs.cpp)

#target_include_directories(catalog
#        PUBLIC ../include)

#target_link_libraries(catalog PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_catalog>
        PARENT_SCOPE)
