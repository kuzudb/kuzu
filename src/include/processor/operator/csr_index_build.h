#pragma once

#include "graph/in_mem_graph.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class CSRIndexBuild : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::CSR_INDEX_BUILD;

public:
    CSRIndexBuild(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, storage::RelTable *relTable,
        DataPos& boundNodeVectorPos, std::shared_ptr<graph::CSRIndexSharedState>& csrIndexSharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, std::unique_ptr<OPPrintInfo> info)
        : Sink{std::move(resultSetDescriptor), operatorType_, std::move(child), id, std::move(info)},
          csrSharedState{csrIndexSharedState}, relTable{relTable},
          boundNodeVectorPos{boundNodeVectorPos} {}

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    uint64_t calculateDegree(ExecutionContext *executionContext);

    void executeInternal(ExecutionContext* context) override;

    std::shared_ptr<graph::CSRIndexSharedState> getCSRSharedState() { return csrSharedState; }

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CSRIndexBuild>(resultSetDescriptor->copy(), relTable,
            boundNodeVectorPos, csrSharedState, children[0]->clone(), id, printInfo->copy());
    }

private:
    std::shared_ptr<graph::CSRIndexSharedState> csrSharedState;

    storage::RelTable* relTable;
    static constexpr common::RelDataDirection direction = common::RelDataDirection::FWD;
    std::vector<common::column_id_t> columnIDs;
    std::unique_ptr<storage::RelTableScanState> fwdReadState;

    DataPos boundNodeVectorPos;           // constructor
    common::ValueVector* boundNodeVector; // initLocalStateInternal
    std::unique_ptr<common::ValueVector> nbrNodeVector;   // initLocalStateInternal
};

} // namespace processor
} // namespace kuzu
