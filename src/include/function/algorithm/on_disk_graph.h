#pragma once

#include "graph.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace graph {

class OnDiskGraph : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const std::string& nodeTableName,
        const std::string& relTableName);

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

    uint64_t getFwdDegreeOffset(common::offset_t offset) override;

    void initializeStateFwdNbrs(common::offset_t offset, NbrScanState *nbrScanState) override;

    bool hasMoreFwdNbrs(NbrScanState *nbrScanState) override;

    common::ValueVector* getFwdNbrs(NbrScanState *nbrScanState) override;

private:
    storage::NodeTable* nodeTable;
    storage::RelTable* relTable;
};

} // namespace graph
} // namespace kuzu
