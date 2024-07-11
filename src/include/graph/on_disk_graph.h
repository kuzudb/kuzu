#pragma once

#include "graph.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace graph {

class OnDiskGraph : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, common::table_id_t nodeTableID,
        common::table_id_t relTableID);

    common::table_id_t getNodeTableID() override { return nodeTable->getTableID(); }

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

    std::vector<common::nodeID_t> getNbrs(common::offset_t offset,
        NbrScanState* nbrScanState) override;

    void initializeStateFwdNbrs(common::offset_t offset, NbrScanState* nbrScanState) override;

    bool hasMoreFwdNbrs(NbrScanState* nbrScanState) override;

    void getFwdNbrs(NbrScanState* nbrScanState) override;

private:
    main::ClientContext* context;
    storage::NodeTable* nodeTable;
    storage::RelTable* relTable;
};

} // namespace graph
} // namespace kuzu
