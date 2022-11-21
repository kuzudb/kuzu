add_library(kuzu_common_csv_reader
        OBJECT
        csv_reader.cpp)

#target_include_directories(common_csv_reader PUBLIC ../../include)
#target_link_libraries(common_csv_reader PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_csv_reader>
        PARENT_SCOPE)
