#pragma once

#include "src/common/include/types.h"
#include "src/storage/include/structures/lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

template<typename T>
class PropertyLists : public Lists {

public:
    PropertyLists(string fname, BufferManager& bufferManager) : Lists{fname, bufferManager} {};
};

typedef PropertyLists<int32_t> RelPropertyListsInt;
typedef PropertyLists<double_t> RelPropertyListsDouble;
typedef PropertyLists<uint8_t> RelPropertyListsBool;

} // namespace storage
} // namespace graphflow
