#include "c_api/kuzu.h"
#include "common/types/types.h"
#include "main/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;

kuzu_logical_type* kuzu_data_type_create(
    kuzu_data_type_id id, kuzu_logical_type* child_type, uint64_t fixed_num_elements_in_list) {
    auto* c_data_type = (kuzu_logical_type*)malloc(sizeof(kuzu_logical_type));
    uint8_t data_type_id_u8 = id;
    LogicalType* data_type;
    auto logicalTypeID = static_cast<LogicalTypeID>(data_type_id_u8);
    if (child_type == nullptr) {
        data_type = new LogicalType(logicalTypeID);
    } else {
        auto child_type_pty =
            std::make_unique<LogicalType>(*static_cast<LogicalType*>(child_type->_data_type));
        auto extraTypeInfo = fixed_num_elements_in_list > 0 ?
                                 std::make_unique<FixedListTypeInfo>(
                                     std::move(child_type_pty), fixed_num_elements_in_list) :
                                 std::make_unique<VarListTypeInfo>(std::move(child_type_pty));
        data_type = new LogicalType(logicalTypeID, std::move(extraTypeInfo));
    }
    c_data_type->_data_type = data_type;
    return c_data_type;
}

kuzu_logical_type* kuzu_data_type_clone(kuzu_logical_type* data_type) {
    auto* c_data_type = (kuzu_logical_type*)malloc(sizeof(kuzu_logical_type));
    c_data_type->_data_type = new LogicalType(*static_cast<LogicalType*>(data_type->_data_type));
    return c_data_type;
}

void kuzu_data_type_destroy(kuzu_logical_type* data_type) {
    if (data_type == nullptr) {
        return;
    }
    if (data_type->_data_type != nullptr) {
        delete static_cast<LogicalType*>(data_type->_data_type);
    }
    free(data_type);
}

bool kuzu_data_type_equals(kuzu_logical_type* data_type1, kuzu_logical_type* data_type2) {
    return *static_cast<LogicalType*>(data_type1->_data_type) ==
           *static_cast<LogicalType*>(data_type2->_data_type);
}

kuzu_data_type_id kuzu_data_type_get_id(kuzu_logical_type* data_type) {
    auto data_type_id_u8 =
        static_cast<uint8_t>(static_cast<LogicalType*>(data_type->_data_type)->getLogicalTypeID());
    return static_cast<kuzu_data_type_id>(data_type_id_u8);
}

uint64_t kuzu_data_type_get_fixed_num_elements_in_list(kuzu_logical_type* data_type) {
    auto parent_type = static_cast<LogicalType*>(data_type->_data_type);
    if (parent_type->getLogicalTypeID() != LogicalTypeID::FIXED_LIST) {
        return 0;
    }
    return FixedListType::getNumElementsInList(static_cast<LogicalType*>(data_type->_data_type));
}
