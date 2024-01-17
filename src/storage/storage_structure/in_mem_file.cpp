#include "storage/storage_structure/in_mem_file.h"

#include <fcntl.h>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/file_system/virtual_file_system.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"

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
        if (length > BufferPoolConstants::PAGE_256KB_SIZE) {
            throw CopyException(ExceptionMessage::overLargeStringPKValueException(length));
        }

        int32_t remainingLength = length;
        // There should always be some space to write. The constructor adds an empty page, and
        // we always add new pages if we run out of space at the end of the following loop
        KU_ASSERT(nextOffsetInPageToAppend < BufferPoolConstants::PAGE_4KB_SIZE);

        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPageIdxToAppend, nextOffsetInPageToAppend);
        while (remainingLength > 0) {
            auto numBytesToWriteInPage = std::min(static_cast<uint64_t>(remainingLength),
                BufferPoolConstants::PAGE_4KB_SIZE - nextOffsetInPageToAppend);
            pages[nextPageIdxToAppend]->write(nextOffsetInPageToAppend,
                (uint8_t*)rawString + (length - remainingLength), numBytesToWriteInPage);
            nextOffsetInPageToAppend += numBytesToWriteInPage;
            remainingLength -= numBytesToWriteInPage;
            // Allocate a new page if necessary.
            if (nextOffsetInPageToAppend >= BufferPoolConstants::PAGE_4KB_SIZE) {
                addANewPage();
                nextOffsetInPageToAppend = 0;
                nextPageIdxToAppend++;
            }
        }
    }
    return result;
}

std::string InMemFile::readString(ku_string_t* strInInMemOvfFile) {
    auto length = strInInMemOvfFile->len;
    if (ku_string_t::isShortString(length)) {
        return strInInMemOvfFile->getAsShortString();
    } else {
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(strInInMemOvfFile->overflowPtr, pageIdx, pagePos);
        std::string result;
        result.reserve(length);
        auto remainingLength = length;
        while (remainingLength > 0) {
            auto numBytesToReadInPage = std::min(static_cast<uint64_t>(remainingLength),
                BufferPoolConstants::PAGE_4KB_SIZE - pagePos);
            result +=
                std::string_view(reinterpret_cast<const char*>(pages[pageIdx++]->data) + pagePos,
                    numBytesToReadInPage);
            // After the first page we always start reading from the beginning of the page.
            pagePos = 0;
            remainingLength -= numBytesToReadInPage;
        }
        return result;
    }
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw CopyException("InMemPages: Empty filename");
    }
    auto lastPage = pages.size();
    // If we didn't write to the last page, don't flush it.
    if (nextOffsetInPageToAppend == 0) {
        lastPage = pages.size() - 1;
    }
    for (auto pageIdx = 0u; pageIdx < lastPage; pageIdx++) {
        fileInfo->writeFile(pages[pageIdx]->data, BufferPoolConstants::PAGE_4KB_SIZE,
            pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

} // namespace storage
} // namespace kuzu
