#include "test/storage/include/file_scanners/string_overflow_file_scanner.h"

namespace graphflow {
namespace storage {

string StringOverflowFileFileScanner::readLargeString(const gf_string_t& str) const {
    PageCursor cursor;
    str.getOverflowPtrInfo(cursor.idx, cursor.offset);
    return string((char*)(buffer.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset), str.len);
}

string StringOverflowFileFileScanner::readString(
    gf_string_t& mainString, const StringOverflowFileFileScanner& stringOverflowFileScanner) {
    if (mainString.len <= gf_string_t::SHORT_STR_LENGTH) {
        return mainString.getAsShortString();
    } else {
        return stringOverflowFileScanner.readLargeString(mainString);
    }
}

} // namespace storage
} // namespace graphflow
