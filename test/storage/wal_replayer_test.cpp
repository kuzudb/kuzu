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
    WALReplayer walReplayer(getWAL(*database), getStorageManager(*database),
        getMemoryManager(*database), getBufferManager(*database), getCatalog(*database),
        WALReplayMode::COMMIT_CHECKPOINT);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
