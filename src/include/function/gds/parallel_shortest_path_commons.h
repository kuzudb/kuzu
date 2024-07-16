#pragma once

#include "common/task_system/task_scheduler.h"
#include "gds.h"
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

typedef std::vector<std::pair<std::unique_ptr<IFEMorsel>, std::shared_ptr<ScheduledTask>>>
    scheduledTaskMap;

struct ParallelShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;

    ParallelShortestPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound)
        : GDSBindData{std::move(nodeInput)}, upperBound{upperBound} {}
    ParallelShortestPathBindData(const ParallelShortestPathBindData& other)
        : GDSBindData{other}, upperBound{other.upperBound} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelShortestPathBindData>(*this);
    }
};

class ParallelShortestPathLocalState : public GDSLocalState {
public:
    explicit ParallelShortestPathLocalState() = default;

    void init(main::ClientContext* clientContext) override {
        auto mm = clientContext->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = std::make_shared<DataChunkState>();
        lengthVector->state = dstNodeIDVector->state;
        outputVectors.push_back(srcNodeIDVector.get());
        outputVectors.push_back(dstNodeIDVector.get());
        outputVectors.push_back(lengthVector.get());
        nbrScanState = std::make_unique<graph::NbrScanState>(mm);
    }

    /*
     * Return the amount of work currently being processed on for this IFEMorsel.
     * Based on the frontier on which BFS extension is being done, or if output is being written.
     * Hold the lock in case the IFEMorsel has not been initialized, the frontier vector needs to
     * be accessed in a thread safe manner.
     */
    inline uint64_t getWork() override {
        if (!ifeMorsel || !ifeMorsel->initializedIFEMorsel) {
            return 0u;
        }
        if (ifeMorsel->isBFSCompleteNoLock()) {
            return ifeMorsel->maxOffset -
                   ifeMorsel->nextDstScanStartIdx.load(std::memory_order_acquire);
        }
        /*
         * This is an approximation of the remaining frontier, it can be either:
         * (1) if frontier is sparse, we subtract the next scan idx from current frontier size
         *     and return the value
         * (2) if frontier is dense, subtract next scan idx from maxOffset (since in this case the
         *     frontier is technically the whole currFrontier array)
         */
        if (ifeMorsel->isSparseFrontier) {
            return ifeMorsel->currentFrontierSize -
                   ifeMorsel->nextScanStartIdx.load(std::memory_order_acquire);
        }
        return ifeMorsel->maxOffset - ifeMorsel->nextScanStartIdx.load(std::memory_order_acquire);
    }

    std::unique_ptr<GDSLocalState> copy() override {
        auto localState = std::make_unique<ParallelShortestPathLocalState>();
        localState->ifeMorsel = ifeMorsel;
        return localState;
    }

public:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    IFEMorsel* ifeMorsel;
};

} // namespace function
} // namespace kuzu
