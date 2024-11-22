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

struct OnDiskGraphNbrScanState {
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

    OnDiskGraphNbrScanState(main::ClientContext* context, storage::RelTable& table,
        std::unique_ptr<storage::RelTableScanState> fwdState,
        std::unique_ptr<storage::RelTableScanState> bwdState)
        : fwdIterator{context, &table, std::move(fwdState)},
          bwdIterator{context, &table, std::move(bwdState)} {}
};

class OnDiskGraphNbrScanStates : public NbrScanState {
    friend class OnDiskGraph;

public:
    NbrScanState::Chunk getChunk() override {
        auto& iter = getInnerIterator();
        return createChunk(iter.getNbrNodes(), iter.getEdges(), iter.getSelVectorUnsafe(),
            propertyVector.get());
    }
    bool next() override;

    void startScan(common::RelDataDirection direction_) {
        this->direction = direction_;
        iteratorIndex = 0;
    }

private:
    const OnDiskGraphNbrScanState::InnerIterator& getInnerIterator() const {
        KU_ASSERT(iteratorIndex < scanStates.size());
        if (direction == common::RelDataDirection::FWD) {
            return scanStates[iteratorIndex].second.fwdIterator;
        } else {
            return scanStates[iteratorIndex].second.bwdIterator;
        }
    }

    OnDiskGraphNbrScanState::InnerIterator& getInnerIterator() {
        return const_cast<OnDiskGraphNbrScanState::InnerIterator&>(
            const_cast<const OnDiskGraphNbrScanStates*>(this)->getInnerIterator());
    }

private:
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;
    std::unique_ptr<common::ValueVector> propertyVector;
    size_t iteratorIndex;
    common::RelDataDirection direction;

    std::unique_ptr<evaluator::ExpressionEvaluator> relPredicateEvaluator;

    explicit OnDiskGraphNbrScanStates(main::ClientContext* context,
        std::span<storage::RelTable*> tableIDs, const GraphEntry& graphEntry,
        std::optional<common::idx_t> edgePropertyIndex = std::nullopt);
    std::vector<std::pair<common::table_id_t, OnDiskGraphNbrScanState>> scanStates;
};

class OnDiskGraphVertexScanState : public VertexScanState {
public:
    OnDiskGraphVertexScanState(main::ClientContext& context, common::table_id_t tableID,
        const std::vector<std::string>& propertyNames);

    void startScan(common::offset_t beginOffset, common::offset_t endOffsetExclusive);

    bool next() override;
    Chunk getChunk() override {
        return createChunk(std::span(&nodeIDVector->getValue<common::nodeID_t>(0), numNodesScanned),
            std::span(propertyVectors.valueVectors));
    }

private:
    common::DataChunk propertyVectors;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<storage::NodeTableScanState> tableScanState;
    const main::ClientContext& context;
    const storage::NodeTable& nodeTable;
    common::offset_t numNodesScanned;
    common::table_id_t tableID;
    common::offset_t currentOffset;
    common::offset_t endOffsetExclusive;
};

class KUZU_API OnDiskGraph final : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const GraphEntry& entry);

    std::vector<common::table_id_t> getNodeTableIDs() override;
    std::vector<common::table_id_t> getRelTableIDs() override;

    common::table_id_map_t<common::offset_t> getNumNodesMap(
        transaction::Transaction* transaction) override;

    common::offset_t getNumNodes(transaction::Transaction* transcation) override;
    common::offset_t getNumNodes(transaction::Transaction* transaction,
        common::table_id_t id) override;

    std::vector<RelTableIDInfo> getRelTableIDInfos() override;

    std::unique_ptr<NbrScanState> prepareScan(common::table_id_t relTableID,
        std::optional<common::idx_t> edgePropertyIndex = std::nullopt) override;
    std::unique_ptr<NbrScanState> prepareMultiTableScanFwd(
        std::span<common::table_id_t> nodeTableIDs) override;
    std::unique_ptr<NbrScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) override;
    std::unique_ptr<VertexScanState> prepareVertexScan(common::table_id_t tableID,
        const std::vector<std::string>& propertiesToScan) override;

    Graph::EdgeIterator scanFwd(common::nodeID_t nodeID, NbrScanState& state) override;
    Graph::EdgeIterator scanBwd(common::nodeID_t nodeID, NbrScanState& state) override;

    VertexIterator scanVertices(common::offset_t beginOffset, common::offset_t endOffsetExclusive,
        VertexScanState& state) override;

private:
    main::ClientContext* context;
    GraphEntry graphEntry;

    common::table_id_map_t<storage::NodeTable*> nodeIDToNodeTable;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToFwdRelTables;
    common::table_id_map_t<common::table_id_map_t<storage::RelTable*>> nodeTableIDToBwdRelTables;
};

} // namespace graph
} // namespace kuzu
