#include "api_test/private_api_test.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/types/date_t.h"
#include "common/types/types.h"
#include "storage/storage_manager.h"
#include "storage/table/rel_table.h"
#include "storage/table/table.h"

namespace kuzu {

using common::Date;
using common::date_t;
using common::nodeID_t;
using common::offset_t;
namespace testing {

class RelDeleteTest : public PrivateApiTest {
    void SetUp() override {
        PrivateApiTest::SetUp();
        conn->query("BEGIN TRANSACTION");
    }

public:
    // doesn't actually delete the node itself
    void detachDeleteNode(std::string_view nodeTable, std::string_view relTable, int64_t nodeID);
};

void RelDeleteTest::detachDeleteNode(std::string_view nodeTableName, std::string_view relTableName,
    int64_t idOfNode) {
    auto* catalog = database->getCatalog();
    const auto& knowsTableEntry =
        catalog
            ->getTableCatalogEntry(conn->getClientContext()->getTransaction(),
                std::string{relTableName})
            ->constCast<catalog::RelGroupCatalogEntry>();
    auto& knowsTable = conn->getClientContext()
                           ->getStorageManager()
                           ->getTable(knowsTableEntry.getRelEntryInfos()[0].oid)
                           ->cast<storage::RelTable>();

    // get internal id of node
    common::internalID_t nodeID;
    {
        auto result = conn->query(common::stringFormat("match (p:{}) where p.id = {} return id(p)",
            nodeTableName, idOfNode));
        ASSERT_EQ(1, result->getNumColumns());
        ASSERT_EQ(1, result->getNumTuples());
        nodeID = result->getNext()->getValue(0)->getValue<common::internalID_t>();
    }

    // init delete state
    const auto srcSharedState = std::make_shared<common::DataChunkState>();
    const auto tempSharedState = std::make_shared<common::DataChunkState>();
    auto srcNodeIDVector =
        std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID());
    auto dstNodeIDVector =
        std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID());
    auto relIDVector = std::make_unique<common::ValueVector>(common::LogicalType::INTERNAL_ID());
    srcNodeIDVector->setState(srcSharedState);
    dstNodeIDVector->setState(tempSharedState);
    relIDVector->setState(tempSharedState);
    srcSharedState->getSelVectorUnsafe().setSelSize(1);
    srcNodeIDVector->setValue(0, nodeID);
    auto detachDeleteState = std::make_unique<storage::RelTableDeleteState>(*srcNodeIDVector,
        *dstNodeIDVector, *relIDVector);

    // perform delete
    knowsTable.detachDelete(conn->getClientContext()->getTransaction(),
        common::RelDataDirection::FWD, detachDeleteState.get());
}

// Delete all edges attached to a node without deleting the node
TEST_F(RelDeleteTest, RelDetachDeleteKnows) {
    {
        auto result =
            conn->query("match (a:person)-[:knows]->(:person) where a.id = 3 return count(*)");
        ASSERT_EQ(1, result->getNumColumns());
        ASSERT_EQ(1, result->getNumTuples());
        ASSERT_EQ(3, result->getNext()->getValue(0)->getValue<int64_t>());
    }

    detachDeleteNode("person", "knows", 3);

    // check to see if outgoing edges were deleted
    {
        auto result =
            conn->query("match (a:person)-[:knows]->(:person) where a.id = 3 return count(*)");
        ASSERT_EQ(1, result->getNumColumns());
        ASSERT_EQ(1, result->getNumTuples());
        EXPECT_EQ(0, result->getNext()->getValue(0)->getValue<int64_t>());
    }

    // check to see if incoming edges were not deleted
    {
        auto result = conn->query(
            "match (a:person)-[:knows]->(b:person) where b.id = 3 return a.id order by a.id asc");
        ASSERT_EQ(1, result->getNumColumns());
        ASSERT_EQ(3, result->getNumTuples());
        EXPECT_EQ(0, result->getNext()->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(2, result->getNext()->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(5, result->getNext()->getValue(0)->getValue<int64_t>());
    }
}

TEST_F(RelDeleteTest, RelDetachDeleteStudyAt) {
    {
        auto result = conn->query(
            "match (a:person)-[:studyAt]->(b:organisation) return a.id, b.id order by a.id asc");
        ASSERT_EQ(2, result->getNumColumns());
        ASSERT_EQ(3, result->getNumTuples());

        auto tup = result->getNext();
        EXPECT_EQ(0, tup->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(1, tup->getValue(1)->getValue<int64_t>());

        tup = result->getNext();
        EXPECT_EQ(2, tup->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(1, tup->getValue(1)->getValue<int64_t>());

        tup = result->getNext();
        EXPECT_EQ(8, tup->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(1, tup->getValue(1)->getValue<int64_t>());
    }

    detachDeleteNode("person", "studyAt", 8);

    {
        auto result = conn->query(
            "match (a:person)-[:studyAt]->(b:organisation) return a.id, b.id order by a.id asc");
        ASSERT_EQ(2, result->getNumColumns());
        ASSERT_EQ(2, result->getNumTuples());

        auto tup = result->getNext();
        EXPECT_EQ(0, tup->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(1, tup->getValue(1)->getValue<int64_t>());

        tup = result->getNext();
        EXPECT_EQ(2, tup->getValue(0)->getValue<int64_t>());
        EXPECT_EQ(1, tup->getValue(1)->getValue<int64_t>());
    }
}

} // namespace testing
} // namespace kuzu
