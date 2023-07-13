#include "common/exception.h"
#include "graph_test/graph_test.h"
#include "storage/copier/npy_reader.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class CopyGcsTest : public DBTest {
    void checkQuery(const std::string& query) { ASSERT(conn->query(query)->isSuccess()); }
};

// The purpose of this test is to make sure that we can copy from a public GCS bucket
TEST_F(CopyGcsTest, CopyGcsAnonymousTest) {
    checkQuery("create node table person \
             (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, \
              isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, \
              registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], \
              usedNames STRING[], courseScoresPerTerm INT64[][], grades INT64[4], height float, \
              PRIMARY KEY (ID));");

    checkQuery("create rel table knows (FROM person TO person, date DATE, meetTime \
                TIMESTAMP, validInterval INTERVAL, comments STRING[], MANY_MANY)");
    checkQuery("COPY person FROM \"gs://anonymous@kuzu-test/tinysnb/vPerson.csv\"(HEADER=true);");
    checkQuery("COPY knows FROM \"gs://anonymous@kuzu-test/tinysnb/eKnows.csv\";");
    ASSERT_EQ(conn->query("MATCH (:person)-[:knows]->(:person) return count(*)")
                  ->getNext()
                  ->getValue(0)
                  ->getValue<int64_t>(),
        1);
}
