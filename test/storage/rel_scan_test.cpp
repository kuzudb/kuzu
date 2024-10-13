#include <cstdint>
#include <random>
#include <vector>

#include "catalog/catalog.h"
#include "common/types/types.h"
#include "graph/graph_entry.h"
#include "graph/on_disk_graph.h"
#include "graph_test/base_graph_test.h"
#include "main/client_context.h"
#include "main_test_helper/private_main_test_helper.h"

using kuzu::common::nodeID_t;

namespace kuzu {
namespace testing {

class RelScanTest : public PrivateApiTest {
public:
    void SetUp() override {
        kuzu::testing::PrivateApiTest::SetUp();
        conn->query("BEGIN TRANSACTION");
        context = getClientContext(*conn);
        catalog = context->getCatalog();
        auto transaction = context->getTx();
        auto nodeTableIDs = catalog->getNodeTableIDs(transaction);
        auto relTableIDs = catalog->getRelTableIDs(transaction);
        entry = std::make_unique<kuzu::graph::GraphEntry>(
            catalog->getTableEntries(transaction, nodeTableIDs),
            catalog->getTableEntries(transaction, relTableIDs));
        graph = std::make_unique<kuzu::graph::OnDiskGraph>(context, *entry);
    }

public:
    std::unique_ptr<kuzu::graph::OnDiskGraph> graph;
    std::unique_ptr<kuzu::graph::GraphEntry> entry;
    main::ClientContext* context;
    catalog::Catalog* catalog;
};

class RelScanTestAmazon : public RelScanTest {
    std::string getInputDir() override {
        return kuzu::testing::TestHelper::appendKuzuRootPath("dataset/snap/amazon0601/csv/");
    }
};

// Test correctness of scan fwd
TEST_F(RelScanTest, ScanFwd) {
    auto tableID = catalog->getTableID(context->getTx(), "person");
    auto relTableID = catalog->getTableID(context->getTx(), "knows");
    auto scanState = graph->prepareScan(relTableID);

    const auto compare = [&](uint64_t node, std::vector<nodeID_t> expected) {
        std::vector<nodeID_t> result;
        for (const auto chunk : graph->scanFwd(nodeID_t{node, tableID}, *scanState)) {
            chunk.selVector.forEach([&](auto i) { result.push_back(chunk.nbrNodes[i]); });
        }
        EXPECT_EQ(result, expected);
        EXPECT_EQ(graph->scanFwd(nodeID_t{node, tableID}, *scanState).collectNbrNodes(), expected);
        result.clear();
        for (const auto chunk : graph->scanBwd(nodeID_t{node, tableID}, *scanState)) {
            chunk.selVector.forEach([&](auto i) { result.push_back(chunk.nbrNodes[i]); });
        }
        EXPECT_EQ(result, expected);
        EXPECT_EQ(graph->scanFwd(nodeID_t{node, tableID}, *scanState).collectNbrNodes(), expected);
    };
    compare(0, {nodeID_t{1, tableID}, nodeID_t{2, tableID}, nodeID_t{3, tableID}});
    compare(1, {nodeID_t{0, tableID}, nodeID_t{2, tableID}, nodeID_t{3, tableID}});
    compare(2, {nodeID_t{0, tableID}, nodeID_t{1, tableID}, nodeID_t{3, tableID}});
}

} // namespace testing
} // namespace kuzu
