#include "processor/result/flat_tuple.h"

#include "c_api/kuzu.h"

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
    } catch (Exception& e) { return nullptr; }
    auto* value = (kuzu_value*)malloc(sizeof(kuzu_value));
    value->_value = _value;
    // We set the ownership of the value to C++, so it will not be deleted if the value is destroyed
    // in C.
    value->_is_owned_by_cpp = true;
    return value;
}

char* kuzu_flat_tuple_to_string(kuzu_flat_tuple* flat_tuple, const uint32_t* columns_width,
    uint64_t columns_width_length, const char* delimiter, uint32_t max_width) {
    auto flat_tuple_shared_ptr = static_cast<std::shared_ptr<FlatTuple>*>(flat_tuple->_flat_tuple);
    std::vector<uint32_t> columns_width_vector;
    for (uint64_t i = 0; i < columns_width_length; i++) {
        columns_width_vector.push_back(columns_width[i]);
    }
    auto string = (*flat_tuple_shared_ptr)->toString(columns_width_vector, delimiter, max_width);
    char* string_c = (char*)malloc(string.size() + 1);
    strcpy(string_c, string.c_str());
    return string_c;
}
