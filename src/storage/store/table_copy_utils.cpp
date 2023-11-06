#include "storage/store/table_copy_utils.h"

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void TableCopyUtils::validateStrLen(uint64_t strLen) {
    if (strLen > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(ExceptionMessage::overLargeStringValueException(strLen));
    }
}

} // namespace storage
} // namespace kuzu
