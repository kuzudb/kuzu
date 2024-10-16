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
    if (flat_tuple->_flat_tuple != nullptr && !flat_tuple->_is_owned_by_cpp) {
        delete static_cast<FlatTuple*>(flat_tuple->_flat_tuple);
    }
}

kuzu_state kuzu_flat_tuple_get_value(kuzu_flat_tuple* flat_tuple, uint64_t index,
    kuzu_value* out_value) {
    auto flat_tuple_ptr = static_cast<FlatTuple*>(flat_tuple->_flat_tuple);
    Value* _value = nullptr;
    try {
        _value = flat_tuple_ptr->getValue(index);
    } catch (Exception& e) {
        return KuzuError;
    }
    out_value->_value = _value;
    // We set the ownership of the value to C++, so it will not be deleted if the value is destroyed
    // in C.
    out_value->_is_owned_by_cpp = true;
    return KuzuSuccess;
}

char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple) {
    auto flat_tuple_ptr = static_cast<FlatTuple*>(flat_tuple->_flat_tuple);
    return convertToOwnedCString(flat_tuple_ptr->toString());
}
