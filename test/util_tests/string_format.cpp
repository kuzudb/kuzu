#include "common/string_format.h"

#include "common/exception/internal.h"
#include "gtest/gtest.h"

using namespace kuzu::common;

TEST(StringFormat, Basic) {
    ASSERT_EQ(
        stringFormat("Some formatted data: {} and {}", 'a', 423), "Some formatted data: a and 423");
}

TEST(StringFormat, Escape) {
    ASSERT_EQ(
        stringFormat("Escape this {{}} but not this {{ }}"), "Escape this {} but not this {{ }}");
    ASSERT_EQ(stringFormat("Escape before and after value works: {{}} {} {{}}", 4),
        "Escape before and after value works: {} 4 {}");
}

TEST(StringFormat, UnmatchedOpenBracket) {
    ASSERT_EQ(stringFormat("Random open brackets are interpreted literally: {4 {} {", "abc"),
        "Random open brackets are interpreted literally: {4 abc {");
}

TEST(StringFormat, TooManyArguments) {
    ASSERT_THROW(stringFormat("Format has no arguments", 4), InternalException);
}

TEST(StringFormat, TooFewArguments) {
    ASSERT_THROW(stringFormat("Format with arguments {}"), InternalException);
}

TEST(StringFormat, Format8BitTypes) {
    enum TestEnum : uint8_t {
        YES,
        NO,
    };
    char literal_character = 'a';
    TestEnum enum_val = TestEnum::NO;
    int8_t signed_int8 = 4;
    ASSERT_EQ(stringFormat("{} {} {}", literal_character, enum_val, signed_int8), "a 1 4");
}

TEST(StringFormat, FormatString) {
    std::string a_string = "abc";
    char a_c_string[] = "def";
    std::string another_string = "ghi";
    std::string_view a_view(another_string.c_str());
    ASSERT_EQ(stringFormat("{} {} {}", a_string, a_c_string, a_view), "abc def ghi");
}

TEST(StringFormat, FormatIntegers) {
    int16_t a = 1;
    uint16_t b = 2;
    int32_t c = 3;
    uint32_t d = 4;
    long e = 5;
    unsigned long f = 6;
    long long g = 7;
    unsigned long long h = 8;
    ASSERT_EQ(stringFormat("{} {} {} {} {} {} {} {}", a, b, c, d, e, f, g, h), "1 2 3 4 5 6 7 8");
}

TEST(StringFormat, FormatFloats) {
    float a = 2.3;
    double b = 5.4;
    ASSERT_EQ(stringFormat("{} {}", a, b), "2.300000 5.400000");
}
