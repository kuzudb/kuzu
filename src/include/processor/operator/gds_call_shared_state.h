#pragma once

#include <stack>

#include "common/mask.h"
#include "common/types/internal_id_util.h"
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

    void addMask(common::table_id_t tableID, std::unique_ptr<common::RoaringBitmapSemiMask> mask) {
        KU_ASSERT(!maskMap.contains(tableID));
        maskMap.insert({tableID, std::move(mask)});
    }

    std::vector<common::RoaringBitmapSemiMask*> getMasks() const {
        std::vector<common::RoaringBitmapSemiMask*> masks;
        for (auto& [_, mask] : maskMap) {
            masks.push_back(mask.get());
        }
        return masks;
    }

    const common::table_id_map_t<std::unique_ptr<common::RoaringBitmapSemiMask>>&
    getMaskMap() const {
        return maskMap;
    }
    bool containsTableID(common::table_id_t tableID) const { return maskMap.contains(tableID); }
    common::RoaringBitmapSemiMask* getOffsetMask(common::table_id_t tableID) const {
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
    common::RoaringBitmapSemiMask* getPinnedMask() const { return pinnedMask; }

    bool valid(common::nodeID_t nodeID) {
        KU_ASSERT(maskMap.contains(nodeID.tableID));
        return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
    }

private:
    common::table_id_map_t<std::unique_ptr<common::RoaringBitmapSemiMask>> maskMap;
    common::RoaringBitmapSemiMask* pinnedMask = nullptr;
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
    common::node_id_map_t<uint64_t>* nodeProp;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph,
        common::offset_t limitNumber)
        : fTable{fTable}, graph{std::move(graph)}, nodeProp{nullptr} {
        if (limitNumber != common::INVALID_LIMIT) {
            counter = std::make_unique<GDSOutputCounter>(limitNumber);
        }
    }
    DELETE_COPY_AND_MOVE(GDSCallSharedState);

    void setInputNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        inputNodeMask = std::move(maskMap);
    }
    void enableInputNodeMask() { inputNodeMask->enable(); }
    std::vector<common::RoaringBitmapSemiMask*> getInputNodeMasks() const {
        return inputNodeMask->getMasks();
    }
    NodeOffsetMaskMap* getInputNodeMaskMap() const { return inputNodeMask.get(); }

    void setOutputNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        outputNodeMask = std::move(maskMap);
    }
    void enableOutputNodeMask() { outputNodeMask->enable(); }
    std::vector<common::RoaringBitmapSemiMask*> getOutputNodeMasks() const {
        return outputNodeMask->getMasks();
    }
    NodeOffsetMaskMap* getOutputNodeMaskMap() const { return outputNodeMask.get(); }

    void setPathNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        pathNodeMask = std::move(maskMap);
    }
    bool hasPathNodeMask() const { return pathNodeMask != nullptr; }
    std::vector<common::RoaringBitmapSemiMask*> getPathNodeMasks() const {
        return pathNodeMask->getMasks();
    }
    NodeOffsetMaskMap* getPathNodeMaskMap() const { return pathNodeMask.get(); }

    FactorizedTable* claimLocalTable(storage::MemoryManager* mm);
    void returnLocalTable(FactorizedTable* table);
    void mergeLocalTables();
    bool exceedLimit() const { return !(counter == nullptr) && counter->exceedLimit(); }

    void setNbrTableIDSet(common::table_id_set_t set) { nbrTableIDSet = set; }
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
    // We implement a local ftable pool to avoid generate many small ftables when running GDS.
    // Alternative solutions are directly writing to global ftable with partition so conflict is
    // minimized. Or we optimize ftable to be more memory efficient when number of tuples is small.
    std::stack<FactorizedTable*> availableLocalTables;
    std::vector<std::shared_ptr<FactorizedTable>> localTables;

    common::table_id_set_t nbrTableIDSet;
};

} // namespace processor
} // namespace kuzu
