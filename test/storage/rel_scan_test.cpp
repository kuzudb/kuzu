#include <random>

#include "catalog/catalog.h"
#include "common/types/internal_id_t.h"
#include "graph/graph_entry.h"
#include "graph/on_disk_graph.h"
#include "graph_test/base_graph_test.h"
#include "gtest/gtest.h"
#include "main/client_context.h"
#include "main_test_helper/private_main_test_helper.h"
#include "transaction/transaction.h"

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
        entry = std::make_unique<kuzu::graph::GraphEntry>(
            catalog->getNodeTableIDs(context->getTx()), catalog->getRelTableIDs(context->getTx()));
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

// Test correctness of a random access scan
TEST_F(RelScanTest, RandomAccessScan) {
    auto tableID = catalog->getTableID(context->getTx(), "person");
    auto relTableID = catalog->getTableID(context->getTx(), "knows");
    std::vector<common::nodeID_t> result{nodeID_t{1, tableID}, nodeID_t{2, tableID},
        nodeID_t{3, tableID}};
    auto scanState = graph->prepareScan(relTableID);
    EXPECT_EQ(graph->scanFwdRandom(nodeID_t{0, tableID}, *scanState), result);
    ASSERT_EQ(graph->scanBwdRandom(nodeID_t{0, tableID}, *scanState), result);
    result = {nodeID_t{0, tableID}, nodeID_t{1, tableID}, nodeID_t{3, tableID}};
    ASSERT_EQ(graph->scanFwdRandom(nodeID_t{2, tableID}, *scanState), result);
    result = {nodeID_t{0, tableID}, nodeID_t{2, tableID}, nodeID_t{3, tableID}};
    EXPECT_EQ(graph->scanFwdRandom(nodeID_t{1, tableID}, *scanState), result);
};

// Tests coverage for many scans (some of which should be larger than the vector size)
TEST_F(RelScanTestAmazon, RandomAccessScanMany) {
    auto tableID = catalog->getTableID(context->getTx(), "account");
    auto relTableID = catalog->getTableID(context->getTx(), "follows");
    std::vector<common::nodeID_t> nodeIDs{};
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, 403393);

    auto scanState = graph->prepareScan(relTableID);
    for (size_t i = 0; i < 1000; i++) {
        auto nodeID = nodeID_t{common::offset_t{dist(rng)}, tableID};
        ASSERT_EQ(graph->scanFwdRandom(nodeID, *scanState), graph->scanFwd(nodeID, *scanState));
        ASSERT_EQ(graph->scanBwdRandom(nodeID, *scanState), graph->scanBwd(nodeID, *scanState));
    }
}

} // namespace testing
} // namespace kuzu
