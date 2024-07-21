#include "storage/db_file_id.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

DBFileID DBFileID::newDataFileID() {
    DBFileID retVal;
    retVal.dbFileType = DBFileType::DATA;
    return retVal;
}

DBFileID DBFileID::newPKIndexFileID(table_id_t tableID) {
    DBFileID retVal;
    retVal.dbFileType = DBFileType::NODE_INDEX;
    retVal.tableID = tableID;
    return retVal;
}

} // namespace storage
} // namespace kuzu
