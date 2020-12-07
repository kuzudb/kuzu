#pragma once

#include <memory>

#include "src/common/include/vector/value_vector.h"

using namespace std;

namespace graphflow {
namespace common {

template<typename T>
class PropertyVector : ValueVector {
public:
    //! Create a value vector of the specified data.
    PropertyVector() {
        values = new T[VECTOR_SIZE];
        buffer = values;
    }

    void reset() { values = buffer; }

    //! Returns the [index] value of the List.
    void get(uint64_t index, T& value) const { value = values[index]; }
    //! Sets the [index] value of the List.
    void set(uint64_t index, T value) { values[index] = value; }

protected:
    T* values;
    T* buffer;
};

} // namespace common
} // namespace graphflow
