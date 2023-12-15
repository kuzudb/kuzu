#include "storage/storage_structure/in_mem_file.h"

#include <fcntl.h>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/file_system/virtual_file_system.h"
#include "common/type_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemFile::InMemFile(std::string filePath, common::VirtualFileSystem* vfs)
    : filePath{std::move(filePath)}, nextPageIdxToAppend{0}, nextOffsetInPageToAppend{0} {
    fileInfo = vfs->openFile(this->filePath, O_CREAT | O_WRONLY);
    addANewPage();
}

uint32_t InMemFile::addANewPage(bool setToZero) {
    auto newPageIdx = pages.size();
    pages.push_back(std::make_unique<InMemPage>());
    if (setToZero) {
        memset(pages[newPageIdx]->data, 0, BufferPoolConstants::PAGE_4KB_SIZE);
    }
    return newPageIdx;
}

ku_string_t InMemFile::appendString(const char* rawString) {
    ku_string_t result;
    auto length = strlen(rawString);
    result.len = length;
    std::memcpy(result.prefix, rawString,
        length <= ku_string_t::SHORT_STR_LENGTH ? length : ku_string_t::PREFIX_LENGTH);
    if (length > ku_string_t::SHORT_STR_LENGTH) {
        if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw CopyException(ExceptionMessage::overLargeStringPKValueException(length));
        }
        // Allocate a new page if necessary.
        if (nextOffsetInPageToAppend + length >= BufferPoolConstants::PAGE_4KB_SIZE) {
            addANewPage();
            nextOffsetInPageToAppend = 0;
            nextPageIdxToAppend++;
        }
        pages[nextPageIdxToAppend]->write(nextOffsetInPageToAppend, (uint8_t*)rawString, length);
        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPageIdxToAppend, nextOffsetInPageToAppend);
        nextOffsetInPageToAppend += length;
    }
    return result;
}

std::string InMemFile::readString(ku_string_t* strInInMemOvfFile) {
    if (ku_string_t::isShortString(strInInMemOvfFile->len)) {
        return strInInMemOvfFile->getAsShortString();
    } else {
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(strInInMemOvfFile->overflowPtr, pageIdx, pagePos);
        return {
            reinterpret_cast<const char*>(pages[pageIdx]->data + pagePos), strInInMemOvfFile->len};
    }
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw CopyException("InMemPages: Empty filename");
    }
    for (auto pageIdx = 0u; pageIdx < pages.size(); pageIdx++) {
        fileInfo->writeFile(pages[pageIdx]->data, BufferPoolConstants::PAGE_4KB_SIZE,
            pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

} // namespace storage
} // namespace kuzu
