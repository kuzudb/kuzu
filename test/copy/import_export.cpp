#include "common/file_system/local_file_system.h"
#include "graph_test/base_graph_test.h"
#include "graph_test/graph_test.h"
#include "main/database.h"

namespace kuzu {
namespace testing {

class ImportExportDBTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(ImportExportDBTest, Test) {
    auto tempDir = TestHelper::getTempDir(getTestGroupAndName());
    auto dbPath = common::LocalFileSystem::joinPath(tempDir, "export.tmp");
    auto result = conn->query("EXPORT DATABASE '" + dbPath + "'");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    result.reset();
    createNewDB();
    result = conn->query("IMPORT DATABASE '" + dbPath + "'");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    result = conn->query("MATCH (a:person)-[e:knows]->(b:person) RETURN COUNT(*)");
    ASSERT_TRUE(result->isSuccess()) << result->getErrorMessage();
    ASSERT_TRUE(result->hasNext());
    ASSERT_EQ(result->getNext()->getValue(0)->getValue<int64_t>(), 14);
}

} // namespace testing
} // namespace kuzu
