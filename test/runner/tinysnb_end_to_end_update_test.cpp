#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class TinySnbUpdateTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 22);
        createConn();
    }
};

TEST_F(TinySnbUpdateTest, SetNodeIntPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.age=70");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.age");
    ASSERT_EQ(result->getNext()->getValue(0)->val.int64Val, 70);
}

TEST_F(TinySnbUpdateTest, SetNodeDoublePropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.eyeSight=1.0");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.eyeSight");
    ASSERT_EQ(result->getNext()->getValue(0)->val.doubleVal, 1.0);
}

TEST_F(TinySnbUpdateTest, SetNodeBoolPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.isStudent=false");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.isStudent");
    ASSERT_EQ(result->getNext()->getValue(0)->val.booleanVal, false);
}

TEST_F(TinySnbUpdateTest, SetNodeDatePropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.birthdate=date('2200-10-10')");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.birthdate");
    ASSERT_EQ(result->getNext()->getValue(0)->val.dateVal, Date::FromDate(2200, 10, 10));
}

TEST_F(TinySnbUpdateTest, SetNodeTimestampPropTest) {
    conn->query(
        "MATCH (a:person) WHERE a.ID=0 SET a.registerTime=timestamp('2200-10-10 12:01:01')");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.registerTime");
    ASSERT_EQ(result->getNext()->getValue(0)->val.timestampVal,
        Timestamp::FromDatetime(Date::FromDate(2200, 10, 10), Time::FromTime(12, 1, 1)));
}

TEST_F(TinySnbUpdateTest, SetNodeIntervalPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.lastJobDuration=interval('1 years 1 days')");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.lastJobDuration");
    string intervalStr = "1 years 1 days";
    ASSERT_EQ(result->getNext()->getValue(0)->val.intervalVal,
        Interval::FromCString(intervalStr.c_str(), intervalStr.length()));
}

TEST_F(TinySnbUpdateTest, SetBothUnflatTest) {
    conn->query("MATCH (a:person) SET a.age=a.ID");
    auto result = conn->query("MATCH (a:person) WHERE a.ID < 4 RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|0", "2|2", "3|3"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbUpdateTest, SetUnflatFlatTest) {
    conn->query("MATCH (a:person)-[:knows]->(b:person) WHERE a.ID=0 SET a.age=b.age");
    auto result = conn->query("MATCH (a:person) WHERE a.ID < 4 RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|20", "2|30", "3|45"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbUpdateTest, SetFlatUnflatTest) {
    conn->query("MATCH (a:person)-[:knows]->(b:person) WHERE b.ID=2 SET b.age=a.age");
    auto result = conn->query("MATCH (a:person) WHERE a.ID < 4 RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|35", "2|20", "3|45"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbUpdateTest, SetBothFlatTest) {
    conn->query("MATCH (a:person)-[:studyAt]->(b:organisation), (a)-[:knows]->(c:person) SET "
                "a.age=b.orgCode");
    auto result = conn->query("MATCH (a:person) WHERE a.ID < 4 RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|325", "2|325", "3|45"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbUpdateTest, SetTwoHopTest) {
    conn->query(
        "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) WHERE b.ID=0 SET a.age=c.age");
    auto result = conn->query("MATCH (a:person) WHERE a.ID < 6 RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|35", "2|20", "3|20", "5|20"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
