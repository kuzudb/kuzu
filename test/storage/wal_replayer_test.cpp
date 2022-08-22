#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/wal_replayer.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

class WALReplayerTests : public DBTest {};

TEST_F(WALReplayerTests, ReplayingUncommittedWALForChekpointErrors) {
    auto walIterator = database->getWAL()->getIterator();
    WALReplayer walReplayer(database->getWAL(), database->getStorageManager(),
        database->getBufferManager(), database->getCatalog(), true /* is checkpointWAL */);
    try {
        walReplayer.replay();
        FAIL();
    } catch (StorageException& e) {
    } catch (Exception& e) { FAIL(); }
}
