#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/wal_replayer.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

class WALReplayerTests : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb/"; }
};

TEST_F(WALReplayerTests, ReplayingUncommittedWALForChekpointErrors) {
    auto walIterator = database->getStorageManager()->getWAL().getIterator();
    WALReplayer walReplayer(
        *database->getStorageManager(), *database->getBufferManager(), true /* is checkpointWAL */);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
