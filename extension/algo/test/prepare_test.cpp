#include "api_test/api_test.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

TEST_F(ApiTest, GDSPrepare) {
#ifndef __STATIC_LINK_EXTENSION_TEST__
    ASSERT_TRUE(conn->query(common::stringFormat("LOAD EXTENSION '{}'",
                                TestHelper::appendKuzuRootPath(
                                    "extension/algo/build/libalgo.kuzu_extension")))
                    ->isSuccess());
#endif
    ASSERT_TRUE(conn->query("CALL PROJECT_GRAPH('PK', ['person'], ['knows'])")->isSuccess());
    auto preparedStatement = conn->prepare("CALL page_rank('PK', dampingFactor := $dp, "
                                           "tolerance := $tl) RETURN node.fName, rank;");
    std::unordered_map<std::string, std::unique_ptr<Value>> params;
    auto result = conn->execute(preparedStatement.get(), std::make_pair(std::string("dp"), 0.56),
        std::make_pair(std::string("tl"), 0.0002));
    auto groundTruth = std::vector<std::string>{"Alice|0.125000", "Bob|0.125000", "Carol|0.125000",
        "Dan|0.125000", "Elizabeth|0.055000", "Farooq|0.070400", "Greg|0.070400",
        "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff|0.055000"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

} // namespace testing
} // namespace kuzu
