#include "common/copier_config/copier_config.h"

#include "common/exception/copy.h"
#include "common/exception/internal.h"
#include "common/type_utils.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {

CopyDescription::FileType CopyDescription::getFileTypeFromExtension(const std::string& extension) {
    CopyDescription::FileType fileType = CopyDescription::fileTypeMap[extension];
    if (fileType == FileType::UNKNOWN) {
        throw CopyException("Unsupported file type " + extension);
    }
    return fileType;
}

} // namespace common
} // namespace kuzu
