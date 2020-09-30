#ifndef GRAPHFLOW_STORAGE_NODEPROPERTY_COLUMN_COLUMN_H_
#define GRAPHFLOW_STORAGE_NODEPROPERTY_COLUMN_COLUMN_H_

#include "src/main/graphflow/utils/types.h"

#include <cstdint>
#include <bits/unique_ptr.h>

using namespace graphflow::utils;

namespace graphflow {
namespace storage {

template<class T>
class Column {

	const static uint32_t MAX_NUM_ELEMENTS_IN_SLOT = INT16_MAX /*2 << 16 -1*/;
	utils::gfLabel_t nodeLabel;
	T **slots;

  public:
	Column(const gfLabel_t nodeLabel, const uint64_t numElements, T nullElement) :
	nodeLabel(nodeLabel) {
		auto numSlots = getSlotIdx(numElements) + 1;
		auto numElementsInLastSlot = getOffsetInSlot(numElements);
		slots = new T *[numSlots];
		for (auto i = 0u; i < numSlots - 1; i++) {
			slots[i] = new T[MAX_NUM_ELEMENTS_IN_SLOT];
			std::fill(slots[i], slots[i] + MAX_NUM_ELEMENTS_IN_SLOT, nullElement);
		}
		slots[numSlots - 1] = new T[numElementsInLastSlot];
		std::fill(slots[numSlots - 1], slots[numSlots - 1] + numElementsInLastSlot, nullElement);
	}

	~Column() {
		for (auto i = 0u; i < sizeof(slots) / sizeof(nullptr); i++) {
			delete[](slots[i]);
		}
		delete[](slots);
	}

	inline T getVal(gfNodeOffset_t offset) {
		return slots[getSlotIdx(offset)][getOffsetInSlot(offset)];
	}

	inline void setVal(gfNodeOffset_t offset, T val) {
		slots[getSlotIdx(offset)][getOffsetInSlot(offset)] = val;
	}

	inline static uint32_t getSlotIdx(gfNodeOffset_t nodeOffset) {
		return nodeOffset / MAX_NUM_ELEMENTS_IN_SLOT;
	}

	inline static uint32_t getOffsetInSlot(gfNodeOffset_t nodeOffset) {
		return nodeOffset % MAX_NUM_ELEMENTS_IN_SLOT;
	}
};

typedef Column<utils::gfInt_t> ColumnInteger;
typedef Column<utils::gfDouble_t> ColumnDouble;
typedef Column<utils::gfBool_t> ColumnBoolean;
typedef Column<utils::gfString_t> ColumnString;

} //namespace storage
} //namespace gf

#endif //GRAPHFLOW_STORAGE_NODEPROPERTY_COLUMN_COLUMN_H_
