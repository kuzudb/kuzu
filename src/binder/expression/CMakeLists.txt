#file(GLOB EXPRESSION_HEADER_LIST CONFIGURE_DEPENDS "${GlobalLocalManager_SOURCE_DIR}/include/binder/expression/*.h")

add_library(
        kuzu_binder_expression
        OBJECT
        existential_subquery_expression.cpp
        expression.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_expression>
        PARENT_SCOPE)

#target_include_directories(binder_expression PUBLIC ../../include)
#
#source_group(
#        TREE "${PROJECT_SOURCE_DIR}/include"
#        PREFIX "Header Files"
#        FILES ${EXPRESSION_HEADER_LIST}
#)
