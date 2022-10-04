#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/wal_replayer.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

class WALReplayerTests : public DBTest {
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(WALReplayerTests, ReplayingUncommittedWALForChekpointErrors) {
    auto walIterator = database->getWAL()->getIterator();
    WALReplayer walReplayer(database->getWAL(), database->getStorageManager(),
        database->getBufferManager(), database->getMemoryManager(), database->getCatalog(),
        true /* is checkpointWAL */);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
