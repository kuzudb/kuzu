#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/wal_replayer.h"

using namespace kuzu::storage;
using namespace kuzu::testing;

class WALReplayerTests : public DBTest {
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
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
