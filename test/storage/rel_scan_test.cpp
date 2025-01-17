#include <cstdint>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/types/date_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "graph/graph_entry.h"
#include "graph/on_disk_graph.h"
#include "main/client_context.h"
#include "main_test_helper/private_main_test_helper.h"

namespace kuzu {

using common::Date;
using common::date_t;
using common::nodeID_t;
using common::offset_t;
namespace testing {

class RelScanTest : public PrivateApiTest {
public:
    void SetUp() override {
        kuzu::testing::PrivateApiTest::SetUp();
        conn->query("BEGIN TRANSACTION");
        context = getClientContext(*conn);
        catalog = context->getCatalog();
        auto transaction = context->getTransaction();
        std::vector<catalog::TableCatalogEntry*> nodeEntries;
        for (auto& entry : catalog->getNodeTableEntries(transaction)) {
            nodeEntries.push_back(entry);
        }
        std::vector<catalog::TableCatalogEntry*> relEntries;
        for (auto& entry : catalog->getRelTableEntries(transaction)) {
            relEntries.push_back(entry);
        }
        auto entry = graph::GraphEntry(nodeEntries, relEntries);
        graph = std::make_unique<kuzu::graph::OnDiskGraph>(context, std::move(entry));

        fwdStorageOnly = (common::ExtendDirectionUtil::getDefaultExtendDirection() ==
                          common::ExtendDirection::FWD);
    }

public:
    std::unique_ptr<kuzu::graph::OnDiskGraph> graph;
    main::ClientContext* context;
    catalog::Catalog* catalog;
    bool fwdStorageOnly;
};

class RelScanTestAmazon : public RelScanTest {
    std::string getInputDir() override {
        return kuzu::testing::TestHelper::appendKuzuRootPath("dataset/snap/amazon0601/csv/");
    }
};

// Test correctness of scan fwd
TEST_F(RelScanTest, ScanFwd) {
    auto tableID = catalog->getTableCatalogEntry(context->getTransaction(), "person")->getTableID();
    auto relEntry = catalog->getTableCatalogEntry(context->getTransaction(), "knows");
    auto scanState = graph->prepareRelScan(relEntry, "date");

    std::unordered_map<offset_t, common::date_t> expectedDates = {
        {0, Date::fromDate(2021, 6, 30)},
        {1, Date::fromDate(2021, 6, 30)},
        {2, Date::fromDate(2021, 6, 30)},
        {3, Date::fromDate(2021, 6, 30)},
        {4, Date::fromDate(1950, 5, 14)},
        {5, Date::fromDate(1950, 5, 14)},
        {6, Date::fromDate(2021, 6, 30)},
        {7, Date::fromDate(1950, 5, 14)},
        {8, Date::fromDate(2000, 1, 1)},
        {9, Date::fromDate(2021, 6, 30)},
        {10, Date::fromDate(1950, 05, 14)},
        {11, Date::fromDate(2000, 1, 1)},
    };

    const auto compare = [&](uint64_t node, common::offset_vec_t expectedNodeOffsets,
                             common::offset_vec_t expectedFwdRelOffsets,
                             common::offset_vec_t expectedBwdRelOffsets) {
        common::offset_vec_t resultNodeOffsets;
        std::vector<common::nodeID_t> expectedNodes;
        std::transform(expectedNodeOffsets.begin(), expectedNodeOffsets.end(),
            std::back_inserter(expectedNodes),
            [&](auto offset) { return nodeID_t{offset, tableID}; });

        common::offset_vec_t resultRelOffsets;
        std::vector<common::date_t> resultDates;
        for (const auto chunk : graph->scanFwd(nodeID_t{node, tableID}, *scanState)) {
            chunk.forEach<common::date_t>([&](auto nbr, auto edgeID, auto date) {
                EXPECT_EQ(nbr.tableID, tableID);
                resultNodeOffsets.push_back(nbr.offset);
                EXPECT_EQ(edgeID.tableID, relEntry->getTableID());
                resultRelOffsets.push_back(edgeID.offset);
                resultDates.push_back(date);
            });
        }
        EXPECT_EQ(resultNodeOffsets, expectedNodeOffsets);
        EXPECT_EQ(resultRelOffsets, expectedFwdRelOffsets);
        for (size_t i = 0; i < resultRelOffsets.size(); i++) {
            EXPECT_EQ(expectedDates[resultRelOffsets[i]], resultDates[i])
                << " Result " << i << " (rel offset " << resultRelOffsets[i] << ") was "
                << Date::toString(resultDates[i]) << " but we expected "
                << Date::toString(expectedDates[resultRelOffsets[i]]);
        }
        EXPECT_EQ(graph->scanFwd(nodeID_t{node, tableID}, *scanState).collectNbrNodes(),
            expectedNodes);

        if (!fwdStorageOnly) {
            resultNodeOffsets.clear();
            resultRelOffsets.clear();
            resultDates.clear();

            for (const auto chunk : graph->scanBwd(nodeID_t{node, tableID}, *scanState)) {
                chunk.forEach<common::date_t>([&](auto nbr, auto edgeID, auto date) {
                    EXPECT_EQ(nbr.tableID, tableID);
                    resultNodeOffsets.push_back(nbr.offset);
                    EXPECT_EQ(edgeID.tableID, relEntry->getTableID());
                    resultRelOffsets.push_back(edgeID.offset);
                    resultDates.push_back(date);
                });
            }
            EXPECT_EQ(resultNodeOffsets, expectedNodeOffsets);
            EXPECT_EQ(resultRelOffsets, expectedBwdRelOffsets);
            for (size_t i = 0; i < resultRelOffsets.size(); i++) {
                EXPECT_EQ(expectedDates[resultRelOffsets[i]], resultDates[i])
                    << " Result " << i << " (rel offset " << resultRelOffsets[i] << ") was "
                    << Date::toString(resultDates[i]) << " but we expected "
                    << Date::toString(expectedDates[resultRelOffsets[i]]);
            }
            EXPECT_EQ(graph->scanFwd(nodeID_t{node, tableID}, *scanState).collectNbrNodes(),
                expectedNodes);
        }
    };
    compare(0, {1, 2, 3}, {0, 1, 2}, {3, 6, 9});
    compare(1, {0, 2, 3}, {3, 4, 5}, {0, 7, 10});
    compare(2, {0, 1, 3}, {6, 7, 8}, {1, 4, 11});
}

TEST_F(RelScanTest, ScanVertexProperties) {
    auto entry = catalog->getTableCatalogEntry(context->getTransaction(), "person");
    std::vector<std::string> properties = {"fname", "height"};
    auto scanState = graph->prepareVertexScan(entry, properties);

    const auto compare = [&](offset_t startNodeOffset, offset_t endNodeOffset,
                             std::vector<std::tuple<offset_t, std::string, float>> expectedNames) {
        std::vector<std::tuple<offset_t, std::string, float>> results;
        for (auto chunk : graph->scanVertices(startNodeOffset, endNodeOffset, *scanState)) {
            for (size_t i = 0; i < chunk.size(); i++) {
                results.push_back(std::make_tuple(chunk.getNodeIDs()[i].offset,
                    chunk.getProperties<common::ku_string_t>(0)[i].getAsString(),
                    chunk.getProperties<float>(1)[i]));
            };
        }
        ASSERT_EQ(results, expectedNames);
    };
    compare(0, 3, {{0, "Alice", 1.731}, {1, "Bob", 0.99}, {2, "Carol", 1.0}});
    compare(1, 3, {{1, "Bob", 0.99}, {2, "Carol", 1.0}});
    compare(2, 4, {{2, "Carol", 1.0}, {3, "Dan", 1.3}});
}

} // namespace testing
} // namespace kuzu
