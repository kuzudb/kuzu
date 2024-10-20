#pragma once

#include <stack>

#include "common/mask.h"
#include "graph/graph.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class NodeOffsetMaskMap {
public:
    void addMask(common::table_id_t tableID,
        std::unique_ptr<common::NodeOffsetLevelSemiMask> mask) {
        KU_ASSERT(!maskMap.contains(tableID));
        maskMap.insert({tableID, std::move(mask)});
    }

    std::vector<common::NodeSemiMask*> getMasks() const;

    bool containsTableID(common::table_id_t tableID) const { return maskMap.contains(tableID); }
    common::NodeOffsetLevelSemiMask* getOffsetMask(common::table_id_t tableID) const {
        KU_ASSERT(containsTableID(tableID));
        return maskMap.at(tableID).get();
    }

    bool valid(common::nodeID_t nodeID) {
        KU_ASSERT(maskMap.contains(nodeID.tableID));
        return maskMap.at(nodeID.tableID)->isMasked(nodeID.offset);
    }

private:
    common::table_id_map_t<std::unique_ptr<common::NodeOffsetLevelSemiMask>> maskMap;
};

struct GDSCallSharedState {
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> fTable;
    std::unique_ptr<graph::Graph> graph;

    GDSCallSharedState(std::shared_ptr<FactorizedTable> fTable, std::unique_ptr<graph::Graph> graph)
        : fTable{fTable}, graph{std::move(graph)} {}
    DELETE_COPY_AND_MOVE(GDSCallSharedState);

    void setInputNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        inputNodeMask = std::move(maskMap);
    }
    std::vector<common::NodeSemiMask*> getInputNodeMasks() const {
        return inputNodeMask->getMasks();
    }
    NodeOffsetMaskMap* getInputNodeMaskMap() const { return inputNodeMask.get(); }

    void setPathNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
        pathNodeMask = std::move(maskMap);
    }
    bool hasPathNodeMask() const { return pathNodeMask != nullptr; }
    std::vector<common::NodeSemiMask*> getPathNodeMasks() const { return pathNodeMask->getMasks(); }
    NodeOffsetMaskMap* getPathNodeMaskMap() const { return pathNodeMask.get(); }

    FactorizedTable* claimLocalTable(storage::MemoryManager* mm);
    void returnLocalTable(FactorizedTable* table);
    void mergeLocalTables();

private:
    std::unique_ptr<NodeOffsetMaskMap> inputNodeMask = nullptr;
    std::unique_ptr<NodeOffsetMaskMap> pathNodeMask = nullptr;
    // We implement a local ftable pool to avoid generate many small ftables when running GDS.
    // Alternative solutions are directly writing to global ftable with partition so conflict is
    // minimized. Or we optimize ftable to be more memory efficient when number of tuples is small.
    std::stack<FactorizedTable*> availableLocalTables;
    std::vector<std::shared_ptr<FactorizedTable>> localTables;
};

} // namespace processor
} // namespace kuzu
