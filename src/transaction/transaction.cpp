#include "src/transaction/include/transaction_manager.h"

namespace graphflow {
namespace transaction {

TransactionStatus Transaction::commit(uint64_t commitId) {
    TransactionStatus status;
    try {
        if (!isReadOnly) {
            // put in-mem pages to the disk.
            // write to the log
            // other cleanups
        }
        status.statusType = COMMITTED;
    } catch (exception& e) {
        status.statusType = FAILED;
        status.msg = "Commit failed: " + string(e.what());
    }
    return status;
}

TransactionStatus Transaction::rollback() {
    TransactionStatus status;
    commitId = INVALID_TXN_ID;
    // cleanups, if any
    status.statusType = ROLLBACKED;
    return status;
}

} // namespace transaction
} // namespace graphflow
