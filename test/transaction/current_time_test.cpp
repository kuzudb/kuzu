#include "common/types/date_t.h"
#include "graph_test/graph_test.h"

namespace kuzu {
namespace testing {

class CurrentTimeTest : public EmptyDBTest {
protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }

    void TearDown() override { EmptyDBTest::TearDown(); }

public:
    common::timestamp_t getCurrentTimestamp() {
        return conn->query("RETURN CURRENT_TIMESTAMP()")
            ->getNext()
            ->getValue(0)
            ->getValue<common::timestamp_t>();
    }

    common::date_t getCurrentDate() {
        return conn->query("RETURN CURRENT_DATE()")
            ->getNext()
            ->getValue(0)
            ->getValue<common::date_t>();
    }
};

TEST_F(CurrentTimeTest, CurrentDate) {
    common::date_t date = getCurrentDate();
    common::date_t date2 = common::Timestamp::getDate(common::Timestamp::getCurrentTimestamp());
    ASSERT_LE(date, date2);

    conn->query("BEGIN TRANSACTION");
    common::date_t date3 = getCurrentDate();
    common::date_t date4 = getCurrentDate();
    ASSERT_EQ(date3, date4);
    conn->query("COMMIT");

    common::date_t date5 = getCurrentDate();
    ASSERT_LE(date4, date5);
}

TEST_F(CurrentTimeTest, CurrentTimestamp) {
    common::timestamp_t timestamp = getCurrentTimestamp();
    common::timestamp_t timestamp2 = common::Timestamp::getCurrentTimestamp();
    ASSERT_LE(timestamp, timestamp2);

    conn->query("BEGIN TRANSACTION");
    common::timestamp_t timestamp3 = getCurrentTimestamp();
    common::timestamp_t timestamp4 = getCurrentTimestamp();
    ASSERT_EQ(timestamp3, timestamp4);
    conn->query("COMMIT");

    common::timestamp_t timestamp5 = getCurrentTimestamp();
    ASSERT_LE(timestamp4, timestamp5);
}

} // namespace testing
} // namespace kuzu
