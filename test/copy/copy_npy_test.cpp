#include "graph_test/graph_test.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::testing;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

class CopyOneDimensionalNpyTest : public DBTest {
public:
    std::string getInputDir() { return TestHelper::appendKuzuRootPath("dataset/npy-1d/"); }
};

class CopyTwoDimensionalNpyTest : public DBTest {
public:
    std::string getInputDir() { return TestHelper::appendKuzuRootPath("dataset/npy-2d/"); }
};

class CopyThreeDimensionalNpyTest : public DBTest {
public:
    std::string getInputDir() { return TestHelper::appendKuzuRootPath("dataset/npy-3d/"); }
};

class CopyLargeNpyTest : public DBTest {
public:
    std::string getInputDir() { return TestHelper::appendKuzuRootPath("dataset/npy-20k/"); }
};

class CopyNpyFaultTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }

    void validateCopyException(std::string copyQuery, std::string expectedException) {
        auto result = conn->query(copyQuery);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_STREQ(result->getErrorMessage().c_str(), expectedException.c_str());
    }
};

TEST_F(CopyOneDimensionalNpyTest, CopyOneDimensionalNpyTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("npytable");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i64");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.int64Val, 1);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.int64Val, 2);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.int64Val, 3);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.int32Val, 1);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.int32Val, 2);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.int32Val, 3);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i16");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.int16Val, 1);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.int16Val, 2);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.int16Val, 3);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f64");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.doubleVal, 1.);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.doubleVal, 2.);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.doubleVal, 3.);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.floatVal, 1.);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.floatVal, 2.);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.floatVal, 3.);
}

TEST_F(CopyTwoDimensionalNpyTest, CopyTwoDimensionalNpyTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("npytable");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "id");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.int64Val, 1);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.int64Val, 2);
    ASSERT_EQ(col->readValueForTestingOnly(2).val.int64Val, 3);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i64");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    auto listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int64Val, 1);
    ASSERT_EQ(listVal[1]->val.int64Val, 2);
    ASSERT_EQ(listVal[2]->val.int64Val, 3);
    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int64Val, 4);
    ASSERT_EQ(listVal[1]->val.int64Val, 5);
    ASSERT_EQ(listVal[2]->val.int64Val, 6);
    listVal = col->readValueForTestingOnly(2).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int64Val, 7);
    ASSERT_EQ(listVal[1]->val.int64Val, 8);
    ASSERT_EQ(listVal[2]->val.int64Val, 9);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int32Val, 1);
    ASSERT_EQ(listVal[1]->val.int32Val, 2);
    ASSERT_EQ(listVal[2]->val.int32Val, 3);
    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int32Val, 4);
    ASSERT_EQ(listVal[1]->val.int32Val, 5);
    ASSERT_EQ(listVal[2]->val.int32Val, 6);
    listVal = col->readValueForTestingOnly(2).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int32Val, 7);
    ASSERT_EQ(listVal[1]->val.int32Val, 8);
    ASSERT_EQ(listVal[2]->val.int32Val, 9);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i16");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int16Val, 1);
    ASSERT_EQ(listVal[1]->val.int16Val, 2);
    ASSERT_EQ(listVal[2]->val.int16Val, 3);
    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int16Val, 4);
    ASSERT_EQ(listVal[1]->val.int16Val, 5);
    ASSERT_EQ(listVal[2]->val.int16Val, 6);
    listVal = col->readValueForTestingOnly(2).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.int16Val, 7);
    ASSERT_EQ(listVal[1]->val.int16Val, 8);
    ASSERT_EQ(listVal[2]->val.int16Val, 9);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f64");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.doubleVal, 1.);
    ASSERT_EQ(listVal[1]->val.doubleVal, 2.);
    ASSERT_EQ(listVal[2]->val.doubleVal, 3.);
    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.doubleVal, 4.);
    ASSERT_EQ(listVal[1]->val.doubleVal, 5.);
    ASSERT_EQ(listVal[2]->val.doubleVal, 6.);
    listVal = col->readValueForTestingOnly(2).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.doubleVal, 7.);
    ASSERT_EQ(listVal[1]->val.doubleVal, 8.);
    ASSERT_EQ(listVal[2]->val.doubleVal, 9.);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.floatVal, 1.);
    ASSERT_EQ(listVal[1]->val.floatVal, 2.);
    ASSERT_EQ(listVal[2]->val.floatVal, 3.);
    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.floatVal, 4.);
    ASSERT_EQ(listVal[1]->val.floatVal, 5.);
    ASSERT_EQ(listVal[2]->val.floatVal, 6.);
    listVal = col->readValueForTestingOnly(2).listVal;
    ASSERT_EQ(listVal.size(), 3);
    ASSERT_EQ(listVal[0]->val.floatVal, 7.);
    ASSERT_EQ(listVal[1]->val.floatVal, 8.);
    ASSERT_EQ(listVal[2]->val.floatVal, 9.);
}

TEST_F(CopyThreeDimensionalNpyTest, CopyThreeDimensionalNpyIntoTwoDimensionaTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("npytable");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "id");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    ASSERT_EQ(col->readValueForTestingOnly(0).val.int64Val, 1);
    ASSERT_EQ(col->readValueForTestingOnly(1).val.int64Val, 2);

    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "i64");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    auto listVal = col->readValueForTestingOnly(0).listVal;
    ASSERT_EQ(listVal.size(), 12);
    ASSERT_EQ(listVal[0]->val.int16Val, 1);
    ASSERT_EQ(listVal[1]->val.int16Val, 2);
    ASSERT_EQ(listVal[2]->val.int16Val, 3);
    ASSERT_EQ(listVal[3]->val.int16Val, 4);
    ASSERT_EQ(listVal[4]->val.int16Val, 5);
    ASSERT_EQ(listVal[5]->val.int16Val, 6);
    ASSERT_EQ(listVal[6]->val.int16Val, 7);
    ASSERT_EQ(listVal[7]->val.int16Val, 8);
    ASSERT_EQ(listVal[8]->val.int16Val, 9);
    ASSERT_EQ(listVal[9]->val.int16Val, 10);
    ASSERT_EQ(listVal[10]->val.int16Val, 11);
    ASSERT_EQ(listVal[11]->val.int16Val, 12);

    listVal = col->readValueForTestingOnly(1).listVal;
    ASSERT_EQ(listVal.size(), 12);
    ASSERT_EQ(listVal[0]->val.int16Val, 13);
    ASSERT_EQ(listVal[1]->val.int16Val, 14);
    ASSERT_EQ(listVal[2]->val.int16Val, 15);
    ASSERT_EQ(listVal[3]->val.int16Val, 16);
    ASSERT_EQ(listVal[4]->val.int16Val, 17);
    ASSERT_EQ(listVal[5]->val.int16Val, 18);
    ASSERT_EQ(listVal[6]->val.int16Val, 19);
    ASSERT_EQ(listVal[7]->val.int16Val, 20);
    ASSERT_EQ(listVal[8]->val.int16Val, 21);
    ASSERT_EQ(listVal[9]->val.int16Val, 22);
    ASSERT_EQ(listVal[10]->val.int16Val, 23);
    ASSERT_EQ(listVal[11]->val.int16Val, 24);
}

TEST_F(CopyLargeNpyTest, CopyLargeNpyTest) {
    auto storageManager = getStorageManager(*database);
    auto catalog = getCatalog(*database);
    auto tableID = catalog->getReadOnlyVersion()->getTableID("npytable");
    auto propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "id");
    auto col =
        storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    for (size_t i = 0; i < 20000; ++i) {
        ASSERT_EQ(col->readValueForTestingOnly(i).val.int64Val, i);
    }
    propertyIdx = catalog->getReadOnlyVersion()->getNodeProperty(tableID, "f32");
    col = storageManager->getNodesStore().getNodePropertyColumn(tableID, propertyIdx.propertyID);
    for (size_t i = 0; i < 200000; ++i) {
        size_t rowIdx = i / 10;
        size_t colIdx = i % 10;
        ASSERT_EQ(col->readValueForTestingOnly(rowIdx).listVal[colIdx]->val.floatVal, (float)i);
    }
}

TEST_F(CopyNpyFaultTest, CopyNpyInsufficientNumberOfProperties) {
    conn->query("create node table npytable (id INT64,i64 INT64[12],PRIMARY KEY(id));");
    validateCopyException("copy npytable from (\"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-3d/three_dim_int64.npy") +
                              "\") by column;",
        "Binder exception: Number of npy files is not equal to number of properties in table "
        "npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyRedundantProperties) {
    conn->query("create node table npytable (id INT64,i32 INT32, PRIMARY KEY(id));");
    auto f32Path = TestHelper::appendKuzuRootPath("dataset/npy-20k/two_dim_float.npy");
    validateCopyException("copy npytable from (\"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-20k/id_int64.npy") +
                              "\", \"" + f32Path + "\") BY COLUMN;",
        "The type of npy file " + f32Path + " does not match the type defined in table npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyVectorIntoScaler) {
    conn->query("create node table npytable (id INT64,f32 FLOAT, PRIMARY KEY(id));");
    auto f32Path = TestHelper::appendKuzuRootPath("dataset/npy-20k/two_dim_float.npy");
    validateCopyException("copy npytable from (\"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-20k/id_int64.npy") +
                              "\", \"" + f32Path + "\") by column;",
        "Cannot copy a vector property in npy file " + f32Path +
            " to a scalar property in table npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyWithMismatchedTypeOneDimensionTest) {
    conn->query("create node table npytable (id INT64,i32 INT32, PRIMARY KEY(id));");
    auto f32Path = TestHelper::appendKuzuRootPath("dataset/npy-1d/one_dim_float.npy");
    validateCopyException("copy npytable from ( \"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-1d/one_dim_int64.npy") +
                              "\", \"" + f32Path + "\") by column;",
        "The type of npy file " + f32Path + " does not match the type defined in table npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyWithMismatchedTypeTwoDimensionTest) {
    conn->query("create node table npytable (id INT64,i32 INT32[10], PRIMARY KEY(id));");
    auto f32Path = TestHelper::appendKuzuRootPath("dataset/npy-20k/two_dim_float.npy");
    validateCopyException("copy npytable from (\"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-20k/id_int64.npy") +
                              "\", \"" + f32Path + "\") by column;",
        "The type of npy file " + f32Path + " does not match the type defined in table npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyWithMismatchedDimensionTest) {
    conn->query("create node table npytable (id INT64,f32 FLOAT[12],PRIMARY KEY(id));");
    auto twoDimFloatNpyPath = TestHelper::appendKuzuRootPath("dataset/npy-20k/two_dim_float.npy");
    validateCopyException("copy npytable from (\"" +
                              TestHelper::appendKuzuRootPath("dataset/npy-20k/id_int64.npy") +
                              "\", \"" + twoDimFloatNpyPath + "\") by column;",
        "The shape of " + twoDimFloatNpyPath +
            " does not match the length of the fixed list property in table npytable.");
}

TEST_F(CopyNpyFaultTest, CopyNpyWithMismatchedLengthTest) {
    conn->query("create node table npytable (id INT64,i64 INT64[12],PRIMARY KEY(id));");
    validateCopyException(
        "copy npytable from (\"" + TestHelper::appendKuzuRootPath("dataset/npy-20k/id_int64.npy") +
            "\", \"" + TestHelper::appendKuzuRootPath("dataset/npy-3d/three_dim_int64.npy") +
            "\") by column;",
        "Number of rows in npy files is not equal to each other.");
}
} // namespace testing
} // namespace kuzu
