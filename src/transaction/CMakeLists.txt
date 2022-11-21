add_library(kuzu_transaction
        OBJECT
        transaction_manager.cpp)

#target_include_directories(transaction PUBLIC ../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_transaction>
        PARENT_SCOPE)
