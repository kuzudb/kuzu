#ifndef GRAPHFLOW_COMMON_LOADER_UTILS_H_
#define GRAPHFLOW_COMMON_LOADER_UTILS_H_

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

class InMemoryColumnBase {
public:
    unique_ptr<uint8_t[]> data;
    size_t size;
    InMemoryColumnBase(size_t size) : data(make_unique<uint8_t[]>(size)), size(size){};
};

template<typename T>
class InMemoryColumn : public InMemoryColumnBase {

public:
    InMemoryColumn(uint64_t numElements) : InMemoryColumnBase(sizeof(T) * numElements){};
    void set(T val, uint64_t offset) {
        memcpy(data.get() + (offset * sizeof(T)), &val, sizeof(T));
    }
};

typedef InMemoryColumn<gfInt_t> RawColumnInteger;

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_UTILS_H_
