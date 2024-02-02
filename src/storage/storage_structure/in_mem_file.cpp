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
    : filePath{std::move(filePath)}, nextPosToAppend(0, 0) {
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

ku_string_t InMemFile::appendString(std::string_view rawString) {
    ku_string_t result;
    auto length = rawString.length();
    result.len = length;
    std::memcpy(result.prefix, rawString.data(),
        length <= ku_string_t::SHORT_STR_LENGTH ? length : ku_string_t::PREFIX_LENGTH);
    if (length > ku_string_t::SHORT_STR_LENGTH) {
        if (length > BufferPoolConstants::PAGE_256KB_SIZE) {
            throw CopyException(ExceptionMessage::overLargeStringPKValueException(length));
        }

        int32_t remainingLength = length;
        // There should always be some space to write. The constructor adds an empty page, and
        // we always add new pages if we run out of space at the end of the following loop
        KU_ASSERT(nextPosToAppend.elemPosInPage < BufferPoolConstants::PAGE_4KB_SIZE);

        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPosToAppend.pageIdx, nextPosToAppend.elemPosInPage);
        while (remainingLength > 0) {
            auto numBytesToWriteInPage = std::min(static_cast<uint64_t>(remainingLength),
                BufferPoolConstants::PAGE_4KB_SIZE - nextPosToAppend.elemPosInPage);
            pages[nextPosToAppend.pageIdx]->write(nextPosToAppend.elemPosInPage,
                reinterpret_cast<const uint8_t*>(rawString.data()) + (length - remainingLength),
                numBytesToWriteInPage);
            remainingLength -= numBytesToWriteInPage;
            // Allocate a new page if necessary.
            nextPosToAppend.elemPosInPage += numBytesToWriteInPage;
            if (nextPosToAppend.elemPosInPage >= common::BufferPoolConstants::PAGE_4KB_SIZE) {
                nextPosToAppend.nextPage();
                addANewPage();
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
        PageCursor cursor;
        TypeUtils::decodeOverflowPtr(
            strInInMemOvfFile->overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
        std::string result;
        result.reserve(length);
        auto remainingLength = length;
        while (remainingLength > 0) {
            auto numBytesToReadInPage = std::min(static_cast<uint64_t>(remainingLength),
                BufferPoolConstants::PAGE_4KB_SIZE - cursor.elemPosInPage);
            result += std::string_view(
                reinterpret_cast<const char*>(pages[cursor.pageIdx]->data) + cursor.elemPosInPage,
                numBytesToReadInPage);
            cursor.nextPage();
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
    if (nextPosToAppend.elemPosInPage == 0) {
        lastPage = pages.size() - 1;
    }
    for (auto pageIdx = 0u; pageIdx < lastPage; pageIdx++) {
        fileInfo->writeFile(pages[pageIdx]->data, BufferPoolConstants::PAGE_4KB_SIZE,
            pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

} // namespace storage
} // namespace kuzu
