#include "processor/result/flat_tuple.h"

#include "c_api/helpers.h"
#include "c_api/kuzu.h"
#include "common/exception/exception.h"

using namespace kuzu::common;
using namespace kuzu::processor;

void kuzu_flat_tuple_destroy(kuzu_flat_tuple* flat_tuple) {
    if (flat_tuple == nullptr) {
        return;
    }
    if (flat_tuple->_flat_tuple != nullptr) {
        auto flat_tuple_shared_ptr =
            static_cast<std::shared_ptr<FlatTuple>*>(flat_tuple->_flat_tuple);
        flat_tuple_shared_ptr->reset();
        delete flat_tuple_shared_ptr;
    }
    free(flat_tuple);
}

kuzu_value* kuzu_flat_tuple_get_value(kuzu_flat_tuple* flat_tuple, uint64_t index) {
    auto flat_tuple_shared_ptr = static_cast<std::shared_ptr<FlatTuple>*>(flat_tuple->_flat_tuple);
    Value* _value;
    try {
        _value = (*flat_tuple_shared_ptr)->getValue(index);
    } catch (Exception& e) {
        return nullptr;
    }
    auto* value = (kuzu_value*)malloc(sizeof(kuzu_value));
    value->_value = _value;
    // We set the ownership of the value to C++, so it will not be deleted if the value is destroyed
    // in C.
    value->_is_owned_by_cpp = true;
    return value;
}

char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple) {
    auto flat_tuple_shared_ptr = static_cast<std::shared_ptr<FlatTuple>*>(flat_tuple->_flat_tuple);
    return convertToOwnedCString((*flat_tuple_shared_ptr)->toString());
}
