#include "storage/storage_structure/in_mem_file.h"

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemFile::InMemFile(std::shared_ptr<FileInfo> fileInfo, std::atomic<page_idx_t>& pageCounter)
    : fileInfo{fileInfo}, nextPosToAppend(0, 0), pageCounter(pageCounter) {}

void InMemFile::addANewPage() {
    page_idx_t newPageIdx = pageCounter.fetch_add(1);
    // Write the index of the next page to the end of the current page, in case a string overflows
    // from one page to another This is always done, as strings are unlikely to end exactly at the
    // end of a page and it's probably not worth the additional complexity to save a few bytes on
    // the occasions that a string does
    if (pages.size() > 0) {
        pages[nextPosToAppend.pageIdx]->write(
            END_OF_PAGE, reinterpret_cast<uint8_t*>(&newPageIdx), sizeof(page_idx_t));
    }
    pages.emplace(newPageIdx, std::make_unique<InMemPage>());
    nextPosToAppend.elemPosInPage = 0;
    nextPosToAppend.pageIdx = newPageIdx;
}

void InMemFile::appendString(std::string_view rawString, ku_string_t& result) {
    auto length = rawString.length();
    result.len = length;
    std::memcpy(result.prefix, rawString.data(),
        length <= ku_string_t::SHORT_STR_LENGTH ? length : ku_string_t::PREFIX_LENGTH);
    if (length > ku_string_t::SHORT_STR_LENGTH) {
        if (length > BufferPoolConstants::PAGE_256KB_SIZE) {
            if constexpr (StorageConstants::TRUNCATE_OVER_LARGE_STRINGS) {
                length = BufferPoolConstants::PAGE_256KB_SIZE;
                result.len = length;
            } else {
                throw CopyException(ExceptionMessage::overLargeStringPKValueException(length));
            }
        }
        if (pages.size() == 0) {
            addANewPage();
        }

        int32_t remainingLength = length;
        // There should always be some space to write. The constructor adds an empty page, and
        // we always add new pages if we run out of space at the end of the following loop
        KU_ASSERT(nextPosToAppend.elemPosInPage < END_OF_PAGE);

        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPosToAppend.pageIdx, nextPosToAppend.elemPosInPage);
        while (remainingLength > 0) {
            auto numBytesToWriteInPage = std::min(static_cast<uint64_t>(remainingLength),
                END_OF_PAGE - nextPosToAppend.elemPosInPage);
            pages[nextPosToAppend.pageIdx]->write(nextPosToAppend.elemPosInPage,
                reinterpret_cast<const uint8_t*>(rawString.data()) + (length - remainingLength),
                numBytesToWriteInPage);
            remainingLength -= numBytesToWriteInPage;
            // Allocate a new page if necessary.
            nextPosToAppend.elemPosInPage += numBytesToWriteInPage;
            if (nextPosToAppend.elemPosInPage >= END_OF_PAGE) {
                addANewPage();
            }
        }
    }
}

std::string InMemFile::readString(ku_string_t* strInInMemOvfFile) const {
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
            auto numBytesToReadInPage = std::min(
                static_cast<uint64_t>(remainingLength), END_OF_PAGE - cursor.elemPosInPage);
            result +=
                std::string_view(reinterpret_cast<const char*>(pages.at(cursor.pageIdx)->data) +
                                     cursor.elemPosInPage,
                    numBytesToReadInPage);
            cursor.elemPosInPage = 0;
            cursor.pageIdx = getNextPageIndex(cursor.pageIdx);
            remainingLength -= numBytesToReadInPage;
        }
        return result;
    }
}

bool InMemFile::equals(std::string_view keyToLookup, const ku_string_t& keyInEntry) const {
    PageCursor cursor;
    TypeUtils::decodeOverflowPtr(keyInEntry.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
    auto lengthRead = 0u;
    while (lengthRead < keyInEntry.len) {
        auto numBytesToCheckInPage = std::min(
            static_cast<uint64_t>(keyInEntry.len) - lengthRead, END_OF_PAGE - cursor.elemPosInPage);
        if (memcmp(keyToLookup.data() + lengthRead,
                getPage(cursor.pageIdx)->data + cursor.elemPosInPage, numBytesToCheckInPage) != 0) {
            return false;
        }
        cursor.elemPosInPage = 0;
        cursor.pageIdx = getNextPageIndex(cursor.pageIdx);
        lengthRead += numBytesToCheckInPage;
    }
    return true;
}

void InMemFile::flush() {
    for (auto& [pageIndex, page] : pages) {
        // actual page index is stored inside of the InMemPage and does not correspond to the index
        // in the vector
        fileInfo->writeFile(page->data, BufferPoolConstants::PAGE_4KB_SIZE,
            pageIndex * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

} // namespace storage
} // namespace kuzu
