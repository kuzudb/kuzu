#include "common/json_common.h"

#include "common/exception/runtime.h"

namespace kuzu {
namespace json_extension {

yyjson_doc* JSONCommon::readDocument(uint8_t* data, uint64_t size, const yyjson_read_flag flg) {
    yyjson_read_err error;
    auto result = readDocumentUnsafe(data, size, flg, &error);
    if (error.code != YYJSON_READ_SUCCESS) {
        throwParseError(reinterpret_cast<const char*>(data), size, error);
    }
    return result;
}

yyjson_doc* JSONCommon::readDocument(const std::string& str, const yyjson_read_flag flg) {
    return readDocument((uint8_t*)str.c_str(), str.size(), flg);
}

void JSONCommon::throwParseError(const char* data, size_t length, yyjson_read_err& err) {
    size_t line = 0, col = 0, chr = 0;
    if (yyjson_locate_pos(data, length, err.pos, &line, &col, &chr)) {
        throw common::RuntimeException(common::stringFormat(
            "Error {} at line {}, column {}, character index {}", err.msg, line, col, chr));
    } else {
        throw common::RuntimeException(
            common::stringFormat("Error {} at byte {}", err.msg, err.pos));
    }
}

} // namespace json_extension
} // namespace kuzu
