#include <string>

#include "common/string_utils.h"
#include "common/types/types_include.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(StringTest, splitBySpace) {
    std::string str = " a b  c\td ";
    std::vector<std::string> result = StringUtils::splitBySpace(str);
    EXPECT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "a");
    EXPECT_EQ(result[1], "b");
    EXPECT_EQ(result[2], "c");
    EXPECT_EQ(result[3], "d");
}

TEST(StringTest, leftTrim) {
    std::string str = "    command ";
    std::string result = StringUtils::ltrim(str);
    EXPECT_EQ(result, "command ");
}

TEST(StringTest, rightTrim) {
    std::string str = " command    ";
    std::string result = StringUtils::rtrim(str);
    EXPECT_EQ(result, " command");
}
