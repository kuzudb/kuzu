#pragma once

#include <memory>

#include "src/common/include/vector/value_vector.h"

using namespace std;

namespace graphflow {
namespace common {

template<typename T>
class PropertyVector : public ValueVector {
public:
    //! Create a value vector of the specified data.
    PropertyVector() { buffer = new T[NODE_SEQUENCE_VECTOR_SIZE]; }
    ~PropertyVector() { delete buffer; }

    T* getValues() { return values; }
    void setValues(T* values) { this->values = values; }
    void reset() { values = buffer; }

    //! Returns the [index] value of the List.
    void get(uint64_t pos, T& value) const { value = values[pos]; }
    //! Sets the [index] value of the List.
    void set(uint64_t pos, T value) { values[pos] = value; }

protected:
    T* values;
    T* buffer;
};

} // namespace common
} // namespace graphflow
