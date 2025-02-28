#pragma once

#include "common/vector/value_vector.h"

namespace kuzu::evaluator {

class LambdaParamEvaluator;

class ListEntryTracker {
public:
    explicit ListEntryTracker(common::ValueVector* listVector);

    common::offset_t getCurDataOffset() const { return getCurListEntry().offset + offsetInList; }
    common::offset_t getNextDataOffset();
    common::list_entry_t getCurListEntry() const {
        return listVector->getValue<common::list_entry_t>(getListEntryPos());
    }
    common::idx_t getListEntryPos() const { return listEntries[listEntryIdx]; }

    bool done() const { return listEntryIdx >= listEntries.size(); }

private:
    void updateListEntry();

    common::ValueVector* listVector;
    common::idx_t listEntryIdx;
    common::offset_t offsetInList;

    // selected pos of each list entry
    std::vector<common::sel_t> listEntries;
};

// TODO(Royi) add comment on how to use
class ListSliceInfo {
public:
    explicit ListSliceInfo(common::ValueVector* listVector)
        : resultSliceOffset(0), listEntryTracker(listVector),
          sliceDataState(std::make_shared<common::DataChunkState>()),
          sliceListEntryState(std::make_shared<common::DataChunkState>()) {
        sliceDataState->setToUnflat();
        sliceDataState->getSelVectorUnsafe().setToFiltered();
        sliceListEntryState->setToUnflat();
        sliceListEntryState->getSelVectorUnsafe().setToFiltered();
    }

    void nextSlice();

    std::vector<std::shared_ptr<common::DataChunkState>> overrideAndSaveParamStates(
        std::span<evaluator::LambdaParamEvaluator*> lambdaParamEvaluators);
    static void restoreParamStates(
        std::span<evaluator::LambdaParamEvaluator*> lambdaParamEvaluators,
        std::vector<std::shared_ptr<common::DataChunkState>> savedStates);

    // use in cases (like list filter) where the output data offset may not correspond to the input
    // data offset
    common::offset_t& getResultSliceOffset() { return resultSliceOffset; }

    bool done() const;

    common::sel_t getSliceSize() const {
        KU_ASSERT(sliceDataState->getSelSize() == sliceListEntryState->getSelSize());
        return sliceDataState->getSelSize();
    }

    // returns {list entry pos, data pos}
    std::pair<common::sel_t, common::sel_t> getPos(common::idx_t i) const {
        return {sliceListEntryState->getSelVector()[i], sliceDataState->getSelVector()[i]};
    }

private:
    void updateSelVector();

    // offset/size refer to the data vector
    common::offset_t resultSliceOffset;

    ListEntryTracker listEntryTracker;

    std::shared_ptr<common::DataChunkState> sliceDataState;
    std::shared_ptr<common::DataChunkState> sliceListEntryState;
};

} // namespace kuzu::evaluator
