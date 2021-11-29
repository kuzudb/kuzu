#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

using namespace graphflow::common;

typedef pair<uint64_t, uint64_t> KeyBlockMergeIdx;
typedef pair<uint32_t, uint32_t> StrColPos;

namespace graphflow {
namespace processor {

// KeyBlockMerger class takes in a vector of memoryBlocks(currently we can only merge two
// memoryBlocks at a time), range of rows that needs to be merged and merge them into one single
// memoryBlock. we need to find and save the positions of string columns and the number of bytes
// from the last string columns in the constructor, so that we don't need to find the string column
// positions when doing row comparison with strings.
class KeyBlockMerger {
public:
    explicit KeyBlockMerger(vector<shared_ptr<ValueVector>>& orderByVectors,
        vector<bool>& isAscOrder, MemoryManager& memoryManager)
        : orderByVectors{orderByVectors}, memoryManager{memoryManager}, isAscOrder{isAscOrder},
          entrySizeInBytes{OrderByKeyEncoder::getEntrySize(orderByVectors)} {
        auto numBytesToCmp = 0u;
        for (auto i = 0u; i < orderByVectors.size(); i++) {
            auto orderByVector = orderByVectors[i];
            numBytesToCmp += OrderByKeyEncoder::getEncodingSize(orderByVector->dataType);
            if (orderByVector->dataType == STRING) {
                strColPos.emplace_back(make_pair(i, numBytesToCmp));
                numBytesToCmp = 0;
            }
        }
    }

    unique_ptr<MemoryBlock> mergeKeyBlocks(
        vector<unique_ptr<MemoryBlock>>& memoryBlocks, vector<KeyBlockMergeIdx>& keyBlockMergeIdx);

    bool compareRowPtr(uint8_t* leftRowPtr, uint8_t* rightRowPtr, uint64_t leftKeyBlockIdx,
        uint64_t rightKeyBlockIdx);

private:
    vector<shared_ptr<ValueVector>>& orderByVectors;
    MemoryManager& memoryManager;
    vector<bool>& isAscOrder;
    uint64_t entrySizeInBytes;
    vector<StrColPos> strColPos;
};

} // namespace processor
} // namespace graphflow
