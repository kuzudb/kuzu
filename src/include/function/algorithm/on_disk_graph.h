#pragma once

#include "graph.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace graph {

class OnDiskGraph : public Graph {
public:
    OnDiskGraph(main::ClientContext* context, const std::vector<std::string>& tableNames);

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

private:
    std::unordered_map<std::string, storage::NodeTable*> nodeTables;
    std::unordered_map<std::string, storage::RelTable*> relTables;
};

} // namespace graph
} // namespace kuzu