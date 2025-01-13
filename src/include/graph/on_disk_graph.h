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

struct OnDiskGraphDirectedNbrScanState {
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

        common::RelDataDirection getDirection() const { return tableScanState->direction; }

    private:
        common::ValueVector& dstVector() const { return *tableScanState->outputVectors[0]; }
        common::ValueVector& relIDVector() const { return *tableScanState->outputVectors[1]; }

        const main::ClientContext* context;
        storage::RelTable* relTable;
        std::unique_ptr<storage::RelTableScanState> tableScanState;
    };

    std::vector<InnerIterator> directedIterators;

    OnDiskGraphDirectedNbrScanState() = default;
    OnDiskGraphDirectedNbrScanState(main::ClientContext* context, storage::RelTable& table,
        std::vector<std::unique_ptr<storage::RelTableScanState>> directedScanStates) {
        for (auto& directedScanState : directedScanStates) {
            directedIterators.emplace_back(context, &table, std::move(directedScanState));
        }
    }

    InnerIterator& getIterator(common::RelDataDirection direction);
};

class OnDiskGraphNbrScanStates : public NbrScanState {
    friend class OnDiskGraph;

public:
    OnDiskGraphNbrScanStates(main::ClientContext* context, catalog::TableCatalogEntry* tableEntry,
        const GraphEntry& graphEntry);
    OnDiskGraphNbrScanStates(main::ClientContext* context, catalog::TableCatalogEntry* tableEntry,
        const GraphEntry& graphEntry, const std::string& propertyName);

    NbrScanState::Chunk getChunk() override {
        auto& iter = getInnerIterator();
        return createChunk(iter.getNbrNodes(), iter.getEdges(), iter.getSelVectorUnsafe(),
            propertyVector.get());
    }
    bool next() override;

    void startScan(common::RelDataDirection direction_) { this->direction = direction_; }

private:
    OnDiskGraphDirectedNbrScanState::InnerIterator& getInnerIterator() {
        return directedScanState.getIterator(direction);
    }

private:
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<common::ValueVector> relIDVector;
    std::unique_ptr<common::ValueVector> propertyVector;
    common::RelDataDirection direction;

    std::unique_ptr<evaluator::ExpressionEvaluator> relPredicateEvaluator;
    common::table_id_t relTableID;
    OnDiskGraphDirectedNbrScanState directedScanState;
};

class OnDiskGraphVertexScanState : public VertexScanState {
public:
    OnDiskGraphVertexScanState(main::ClientContext& context, catalog::TableCatalogEntry* tableEntry,
        const std::vector<std::string>& propertyNames);

    void startScan(common::offset_t beginOffset, common::offset_t endOffsetExclusive);

    bool next() override;
    Chunk getChunk() override {
        return createChunk(std::span(&nodeIDVector->getValue<common::nodeID_t>(0), numNodesScanned),
            std::span(propertyVectors.valueVectors));
    }

private:
    const main::ClientContext& context;
    const storage::NodeTable& nodeTable;

    common::DataChunk propertyVectors;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<storage::NodeTableScanState> tableScanState;

    common::offset_t numNodesScanned;
    common::offset_t currentOffset;
    common::offset_t endOffsetExclusive;
};

class KUZU_API OnDiskGraph final : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const GraphEntry& entry);

    GraphEntry* getGraphEntry() override { return &graphEntry; }

    std::vector<common::table_id_t> getNodeTableIDs() const override;
    std::vector<common::table_id_t> getRelTableIDs() const override;

    common::table_id_map_t<common::offset_t> getNumNodesMap(
        transaction::Transaction* transaction) const override;

    common::offset_t getNumNodes(transaction::Transaction* transaction) const override;
    common::offset_t getNumNodes(transaction::Transaction* transaction,
        common::table_id_t id) const override;

    std::vector<RelFromToEntryInfo> getRelFromToEntryInfos() override;

    std::unique_ptr<NbrScanState> prepareRelScan(catalog::TableCatalogEntry* tableEntry,
        const std::string& property) override;
    std::unique_ptr<VertexScanState> prepareVertexScan(catalog::TableCatalogEntry* tableEntry,
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
