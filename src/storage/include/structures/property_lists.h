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

typedef PropertyLists<gfInt_t> RelPropertyListsInteger;
typedef PropertyLists<gfDouble_t> RelPropertyListsDouble;
typedef PropertyLists<gfBool_t> RelPropertyListsBoolean;

} // namespace storage
} // namespace graphflow