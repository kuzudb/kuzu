#pragma once

#include <mutex>
#include <stack>

#include "common/mask.h"
#include "graph/graph.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class NodeOffsetMaskMap {
public:
    NodeOffsetMaskMap() : enabled_{false} {}

    void enable() { enabled_ = true; }
    bool enabled() const { return enabled_; }

    common::offset_t getNumMaskedNode() const;

    void addMask(common::table_id_t tableID, std::unique_ptr<common::semi_mask_t> mask) {
        KU_ASSERT(!maskMap.contains(tableID));
        maskMap.insert({tableID, std::move(mask)});
    }

    common::table_id_map_t<common::semi_mask_t*> getMasks() const {
        common::table_id_map_t<common::semi_mask_t*> result;
        for (auto& [tableID, mask] : maskMap) {
            result.emplace(tableID, mask.get());
        }
        return result;
    }

    bool containsTableID(common::table_id_t tableID) const { return maskMap.contains(tableID); }
    common::semi_mask_t* getOffsetMask(common::table_id_t tableID) const {
        KU_ASSERT(containsTableID(tableID));
        return maskMap.at(tableID).get();
    }

    void pin(common::table_id_t tableID) {
        if (maskMap.contains(tableID)) {
            pinnedMask = maskMap.at(tableID).get();
        } else {
            pinnedMask = nullptr;
        }
    }
    bool hasPinnedMask() const { return pinnedMask != nullptr; }
    common::semi_mask_t* getPinnedMask() const { return pinnedMask; }

    bool valid(common::offset_t offset) {
        KU_ASSERT(pinnedMask != nullptr);
        return pinnedMask->isMasked(offset);
    }
    bool valid(common::nodeID_t nodeID) {
        KU_ASSERT(maskMap.contains(nodeID.tableID));
        return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
    }

private:
    common::table_id_map_t<std::unique_ptr<common::semi_mask_t>> maskMap;
    common::semi_mask_t* pinnedMask = nullptr;
    // If mask map is enabled, then some nodes might be masked.
    bool enabled_;
};

class GDSOutputCounter {
public:
    explicit GDSOutputCounter(common::offset_t limitNumber) : limitNumber{limitNumber} {
        counter.store(0);
    }

    void increase(common::offset_t number) { counter.fetch_add(number); }

    bool exceedLimit() const { return counter.load() >= limitNumber; }

private:
    common::offset_t limitNumber;
    std::atomic<common::offset_t> counter;
};

struct KUZU_API GDSCallSharedState {
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> fTable;
    std::unique_ptr<graph::Graph> graph;
    std::unique_ptr<GDSOutputCounter> counter = nullptr;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph)
        : GDSCallSharedState{std::move(fTable), std::move(graph), common::INVALID_LIMIT} {}
    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph,
        common::offset_t limitNumber)
        : fTable{std::move(fTable)}, graph{std::move(graph)} {
        if (limitNumber != common::INVALID_LIMIT) {
            counter = std::make_unique<GDSOutputCounter>(limitNumber);
        }
    }
    DELETE_COPY_AND_MOVE(GDSCallSharedState);

    void setInputNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        inputNodeMask = std::move(maskMap);
    }
    void enableInputNodeMask() { inputNodeMask->enable(); }
    NodeOffsetMaskMap* getInputNodeMaskMap() const { return inputNodeMask.get(); }

    void setOutputNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        outputNodeMask = std::move(maskMap);
    }
    void enableOutputNodeMask() { outputNodeMask->enable(); }
    NodeOffsetMaskMap* getOutputNodeMaskMap() const { return outputNodeMask.get(); }

    void setPathNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        pathNodeMask = std::move(maskMap);
    }
    NodeOffsetMaskMap* getPathNodeMaskMap() const { return pathNodeMask.get(); }

    void setGraphNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap);
    bool hasGraphNodeMask() const { return graphNodeMask != nullptr; }
    NodeOffsetMaskMap* getGraphNodeMaskMap() const { return graphNodeMask.get(); }

    FactorizedTable* claimLocalTable(storage::MemoryManager* mm);
    void returnLocalTable(FactorizedTable* table);
    void mergeLocalTables();
    bool exceedLimit() const { return !(counter == nullptr) && counter->exceedLimit(); }

    void setNbrTableIDSet(common::table_id_set_t set) { nbrTableIDSet = std::move(set); }
    bool inNbrTableIDs(common::table_id_t tableID) const {
        if (nbrTableIDSet.empty()) {
            return true;
        }
        return nbrTableIDSet.contains(tableID);
    }

private:
    std::unique_ptr<NodeOffsetMaskMap> inputNodeMask = nullptr;
    std::unique_ptr<NodeOffsetMaskMap> outputNodeMask = nullptr;
    std::unique_ptr<NodeOffsetMaskMap> pathNodeMask = nullptr;
    std::unique_ptr<NodeOffsetMaskMap> graphNodeMask = nullptr;

    // We implement a local ftable pool to avoid generate many small ftables when running GDS.
    // Alternative solutions are directly writing to global ftable with partition so conflict is
    // minimized. Or we optimize ftable to be more memory efficient when number of tuples is small.
    std::stack<FactorizedTable*> availableLocalTables;
    std::vector<std::shared_ptr<FactorizedTable>> localTables;

    common::table_id_set_t nbrTableIDSet;
};

} // namespace processor
} // namespace kuzu
