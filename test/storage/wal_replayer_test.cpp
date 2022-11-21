#include "storage/wal_replayer.h"
#include "test_helper/test_helper.h"

using namespace kuzu::storage;
using namespace kuzu::testing;

class WALReplayerTests : public DBTest {
    string getInputCSVDir() override { return TestHelper::appendKuzuRootPath("dataset/tinysnb/"); }
};

TEST_F(WALReplayerTests, ReplayingUncommittedWALForChekpointErrors) {
    auto walIterator = getWAL(*database)->getIterator();
    WALReplayer walReplayer(getWAL(*database), getStorageManager(*database),
        getBufferManager(*database), getMemoryManager(*database), getCatalog(*database),
        true /* is checkpointWAL */);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
