#include "graph_test/graph_test.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::testing;

class WALReplayerTests : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(WALReplayerTests, ReplayingUncommittedWALForChekpointErrors) {
    auto walIterator = getWAL(*database)->getIterator();
    WALReplayer walReplayer(getWAL(*database), getStorageManager(*database),
        getMemoryManager(*database), getCatalog(*database), true /* is checkpointWAL */);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
