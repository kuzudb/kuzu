#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class TinySnbUpdateTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        createDBAndConn();
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

TEST_F(TinySnbUpdateTest, SetNodeShortStringPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.fName='abcdef'");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.fName");
    ASSERT_EQ(result->getNext()->getValue(0)->val.strVal.getAsString(), "abcdef");
}

TEST_F(TinySnbUpdateTest, SetNodeLongStringPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.fName='abcdefghijklmnopqrstuvwxyz'");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.fName");
    ASSERT_EQ(
        result->getNext()->getValue(0)->val.strVal.getAsString(), "abcdefghijklmnopqrstuvwxyz");
}

TEST_F(TinySnbUpdateTest, SetNodeListOfIntPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.workedHours=[10,20]");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.workedHours");
    auto value = result->getNext()->getValue(0);
    ASSERT_EQ(TypeUtils::toString(value->val.listVal, value->dataType), "[10,20]");
}

TEST_F(TinySnbUpdateTest, SetNodeListOfShortStringPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.usedNames=['intel','microsoft']");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.usedNames");
    auto value = result->getNext()->getValue(0);
    ASSERT_EQ(TypeUtils::toString(value->val.listVal, value->dataType), "[intel,microsoft]");
}

TEST_F(TinySnbUpdateTest, SetNodeListOfLongStringPropTest) {
    conn->query(
        "MATCH (a:person) WHERE a.ID=0 SET a.usedNames=['abcndwjbwesdsd','microsofthbbjuwgedsd']");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.usedNames");
    auto value = result->getNext()->getValue(0);
    ASSERT_EQ(TypeUtils::toString(value->val.listVal, value->dataType),
        "[abcndwjbwesdsd,microsofthbbjuwgedsd]");
}

TEST_F(TinySnbUpdateTest, SetNodeListofListPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=8 SET a.courseScoresPerTerm=[[10,20],[0,0,0]]");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=8 RETURN a.courseScoresPerTerm");
    auto value = result->getNext()->getValue(0);
    ASSERT_EQ(TypeUtils::toString(value->val.listVal, value->dataType), "[[10,20],[0,0,0]]");
}

TEST_F(TinySnbUpdateTest, SetVeryLongListErrorsTest) {
    conn->beginWriteTransaction();
    string veryLongList = "[";
    for (int i = 0; i < 599; ++i) {
        veryLongList += to_string(i);
        veryLongList += ",";
    }
    veryLongList += "599]";
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 SET a.fName=" + veryLongList);
    ASSERT_FALSE(result->isSuccess());
}

TEST_F(TinySnbUpdateTest, SetNodeIntervalPropTest) {
    conn->query("MATCH (a:person) WHERE a.ID=0 SET a.lastJobDuration=interval('1 years 1 days')");
    auto result = conn->query("MATCH (a:person) WHERE a.ID=0 RETURN a.lastJobDuration");
    string intervalStr = "1 years 1 days";
    ASSERT_EQ(result->getNext()->getValue(0)->val.intervalVal,
        Interval::FromCString(intervalStr.c_str(), intervalStr.length()));
}

TEST_F(TinySnbUpdateTest, SetNodePropNullTest) {
    conn->query("MATCH (a:person) SET a.age=null");
    auto result = conn->query("MATCH (a:person) RETURN a.age");
    auto groundTruth = vector<string>{"", "", "", "", "", "", "", ""};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
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

TEST_F(TinySnbUpdateTest, SetTwoHopNullTest) {
    conn->query("MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) SET a.age=null");
    auto result = conn->query("MATCH (a:person) RETURN a.ID, a.age");
    auto groundTruth = vector<string>{"0|", "10|83", "2|", "3|", "5|", "7|20", "8|25", "9|40"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

TEST_F(TinySnbUpdateTest, DeleteNodeWithEdgeErrorTest) {
    auto result = conn->query("MATCH (a:person) WHERE a.ID = 10 DELETE a;");
    ASSERT_FALSE(result->isSuccess());
}

// NOTE: this query should error once we enforce primary key to be unique
TEST_F(TinySnbUpdateTest, InsertNodeTest) {
    conn->beginWriteTransaction(); // TODO(Semih): check why auto-commit doesn't work
    unique_ptr<QueryResult> result;
    result = conn->query("MATCH (:person) CREATE (:person {ID:100});");
    assert(result->isSuccess());
    result = conn->query("MATCH (:person) RETURN COUNT(*)");
    auto groundTruth = vector<string>{"16"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}

// NOTE: this query should error once we enforce primary key to be unique
TEST_F(TinySnbUpdateTest, InsertNodeTest2) {
    conn->beginWriteTransaction(); // TODO(Semih): check why auto-commit doesn't work
    unique_ptr<QueryResult> result;
    result = conn->query("MATCH (:person)-[:knows]->(:person) CREATE (:person {ID:100});");
    assert(result->isSuccess());
    result = conn->query("MATCH (:person) RETURN COUNT(*)");
    auto groundTruth = vector<string>{"22"};
    ASSERT_EQ(TestHelper::convertResultToString(*result), groundTruth);
}
