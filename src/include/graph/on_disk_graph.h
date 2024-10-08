#pragma once

#include <cstddef>

#include "common/assert.h"
#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
#include "common/enums/rel_direction.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "graph.h"
#include "graph_entry.h"
#include "main/client_context.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {
class MemoryManager;
}
namespace graph {

struct OnDiskGraphScanState {
    class InnerIterator {
    public:
        InnerIterator(const main::ClientContext* context, storage::RelTable* relTable,
            std::unique_ptr<storage::RelTableScanState> tableScanState);

        DELETE_COPY_DEFAULT_MOVE(InnerIterator);

        std::span<const common::nodeID_t> getNbrNodes() const {
            RUNTIME_CHECK(for (size_t i = 0; i < getSelVector().getSelSize(); i++) {
                KU_ASSERT(
                    getSelVector().getSelectedPositions()[i] < common::DEFAULT_VECTOR_CAPACITY);
            });
            return std::span<const common::nodeID_t>(
                &dstVector().getValue<const common::nodeID_t>(0), common::DEFAULT_VECTOR_CAPACITY);
        }
        std::span<const common::nodeID_t> getEdges() const {
            RUNTIME_CHECK(for (size_t i = 0; i < getSelVector().getSelSize(); i++) {
                KU_ASSERT(
                    getSelVector().getSelectedPositions()[i] < common::DEFAULT_VECTOR_CAPACITY);
            });
            return std::span<const common::nodeID_t>(
                &relIDVector().getValue<const common::nodeID_t>(0),
                common::DEFAULT_VECTOR_CAPACITY);
        }

        common::SelectionVector& getSelVectorUnsafe() {
            return tableScanState->outState->getSelVectorUnsafe();
        }

        const common::SelectionVector& getSelVector() const {
            return tableScanState->outState->getSelVector();
        }

        bool next(evaluator::ExpressionEvaluator* predicate);
        void initScan();

    private:
        common::ValueVector& dstVector() const { return *tableScanState->outputVectors[0]; }
        common::ValueVector& relIDVector() const { return *tableScanState->outputVectors[1]; }

        const main::ClientContext* context;
        storage::RelTable* relTable;
        std::unique_ptr<storage::RelTableScanState> tableScanState;
    };

    InnerIterator fwdIterator;
    InnerIterator bwdIterator;

    OnDiskGraphScanState(main::ClientContext* context, storage::RelTable& table,
        std::unique_ptr<storage::RelTableScanState> fwdState,
        std::unique_ptr<storage::RelTableScanState> bwdState)
        : fwdIterator{context, &table, std::move(fwdState)},
          bwdIterator{context, &table, std::move(bwdState)} {}
};

class OnDiskGraphScanStates : public GraphScanState {
    friend class OnDiskGraph;

public:
    GraphScanState::Chunk getChunk() override {
        auto& iter = getInnerIterator();
        return Chunk{iter.getNbrNodes(), iter.getEdges(), iter.getSelVectorUnsafe()};
    }
    bool next() override;

    void startScan(common::RelDataDirection direction_) {
        this->direction = direction_;
        iteratorIndex = 0;
    }

private:
    const OnDiskGraphScanState::InnerIterator& getInnerIterator() const {
        KU_ASSERT(iteratorIndex < scanStates.size());
        if (direction == common::RelDataDirection::FWD) {
            return scanStates[iteratorIndex].second.fwdIterator;
        } else {
            return scanStates[iteratorIndex].second.bwdIterator;
        }
    }

    OnDiskGraphScanState::InnerIterator& getInnerIterator() {
        return const_cast<OnDiskGraphScanState::InnerIterator&>(
            const_cast<const OnDiskGraphScanStates*>(this)->getInnerIterator());
    }

private:
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;
    size_t iteratorIndex;
    common::RelDataDirection direction;

    std::unique_ptr<evaluator::ExpressionEvaluator> relPredicateEvaluator;

    explicit OnDiskGraphScanStates(main::ClientContext* context,
        std::span<storage::RelTable*> tableIDs, const GraphEntry& graphEntry);
    std::vector<std::pair<common::table_id_t, OnDiskGraphScanState>> scanStates;
};

class OnDiskGraph final : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const GraphEntry& entry);

    std::vector<common::table_id_t> getNodeTableIDs() override;
    std::vector<common::table_id_t> getRelTableIDs() override;

    std::unordered_map<common::table_id_t, uint64_t> getNodeTableIDAndNumNodes() override;

    common::offset_t getNumNodes() override;
    common::offset_t getNumNodes(common::table_id_t id) override;

    std::vector<RelTableIDInfo> getRelTableIDInfos() override;

    std::unique_ptr<GraphScanState> prepareScan(common::table_id_t relTableID) override;
    std::unique_ptr<GraphScanState> prepareMultiTableScanFwd(
        std::span<common::table_id_t> nodeTableIDs) override;
    std::unique_ptr<GraphScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) override;

    Graph::Iterator scanFwd(common::nodeID_t nodeID, GraphScanState& state) override;
    Graph::Iterator scanBwd(common::nodeID_t nodeID, GraphScanState& state) override;

private:
    main::ClientContext* context;
    GraphEntry graphEntry;

    common::table_id_map_t<storage::NodeTable*> nodeIDToNodeTable;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToFwdRelTables;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToBwdRelTables;
};

} // namespace graph
} // namespace kuzu
