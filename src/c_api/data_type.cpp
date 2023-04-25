#include "c_api/kuzu.h"
#include "common/types/types.h"
#include "main/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;

kuzu_data_type* kuzu_data_type_create(
    kuzu_data_type_id id, kuzu_data_type* child_type, uint64_t fixed_num_elements_in_list) {
    auto* c_data_type = (kuzu_data_type*)malloc(sizeof(kuzu_data_type));
    uint8_t data_type_id_u8 = id;
    DataType* data_type;
    if (child_type == nullptr) {
        data_type = new DataType(static_cast<DataTypeID>(data_type_id_u8));
    } else {
        auto child_type_pty =
            std::make_unique<DataType>(*static_cast<DataType*>(child_type->_data_type));
        data_type = fixed_num_elements_in_list > 0 ?
                        new DataType(std::move(child_type_pty), fixed_num_elements_in_list) :
                        new DataType(std::move(child_type_pty));
    }
    c_data_type->_data_type = data_type;
    return c_data_type;
}

kuzu_data_type* kuzu_data_type_clone(kuzu_data_type* data_type) {
    auto* c_data_type = (kuzu_data_type*)malloc(sizeof(kuzu_data_type));
    c_data_type->_data_type = new DataType(*static_cast<DataType*>(data_type->_data_type));
    return c_data_type;
}

void kuzu_data_type_destroy(kuzu_data_type* data_type) {
    if (data_type == nullptr) {
        return;
    }
    if (data_type->_data_type != nullptr) {
        delete static_cast<DataType*>(data_type->_data_type);
    }
    free(data_type);
}

bool kuzu_data_type_equals(kuzu_data_type* data_type1, kuzu_data_type* data_type2) {
    return *static_cast<DataType*>(data_type1->_data_type) ==
           *static_cast<DataType*>(data_type2->_data_type);
}

kuzu_data_type_id kuzu_data_type_get_id(kuzu_data_type* data_type) {
    auto data_type_id_u8 =
        static_cast<uint8_t>(static_cast<DataType*>(data_type->_data_type)->getTypeID());
    return static_cast<kuzu_data_type_id>(data_type_id_u8);
}

kuzu_data_type* kuzu_data_type_get_child_type(kuzu_data_type* data_type) {
    auto parent_type = static_cast<DataType*>(data_type->_data_type);
    if (parent_type->getTypeID() != DataTypeID::FIXED_LIST &&
        parent_type->getTypeID() != DataTypeID::VAR_LIST) {
        return nullptr;
    }
    auto child_type = static_cast<DataType*>(data_type->_data_type)->getChildType();
    if (child_type == nullptr) {
        return nullptr;
    }
    auto* child_type_c = (kuzu_data_type*)malloc(sizeof(kuzu_data_type));
    child_type_c->_data_type = new DataType(*child_type);
    return child_type_c;
}

uint64_t kuzu_data_type_get_fixed_num_elements_in_list(kuzu_data_type* data_type) {
    auto parent_type = static_cast<DataType*>(data_type->_data_type);
    if (parent_type->getTypeID() != DataTypeID::FIXED_LIST) {
        return 0;
    }
    auto extra_info = static_cast<DataType*>(data_type->_data_type)->getExtraTypeInfo();
    if (extra_info == nullptr) {
        return 0;
    }
    auto fixed_list_info = dynamic_cast<FixedListTypeInfo*>(extra_info);
    return fixed_list_info->getFixedNumElementsInList();
}
