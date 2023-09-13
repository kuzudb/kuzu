// Copyright 2011-2020 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#include "byte_source.h"

#include "system.h"

#include "serd.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

SerdStatus
serd_byte_source_page(SerdByteSource* const source)
{
  source->read_head = 0;
  const size_t n_read =
    source->read_func(source->file_buf, 1, source->page_size, source->stream);

  if (n_read == 0) {
    source->file_buf[0] = '\0';
    source->eof         = true;
    return (source->error_func(source->stream) ? SERD_ERR_UNKNOWN
                                               : SERD_FAILURE);
  }

  if (n_read < source->page_size) {
    source->file_buf[n_read] = '\0';
    source->buf_size         = n_read;
  }

  return SERD_SUCCESS;
}

SerdStatus
serd_byte_source_open_source(SerdByteSource* const     source,
                             const SerdSource          read_func,
                             const SerdStreamErrorFunc error_func,
                             void* const               stream,
                             const uint8_t* const      name,
                             const size_t              page_size)
{
  const Cursor cur = {name, 1, 1};

  memset(source, '\0', sizeof(*source));
  source->stream      = stream;
  source->from_stream = true;
  source->page_size   = page_size;
  source->buf_size    = page_size;
  source->cur         = cur;
  source->error_func  = error_func;
  source->read_func   = read_func;

  if (page_size > 1) {
    source->file_buf = (uint8_t*)serd_allocate_buffer(page_size);
    source->read_buf = source->file_buf;
    memset(source->file_buf, '\0', page_size);
  } else {
    source->read_buf = &source->read_byte;
  }

  return SERD_SUCCESS;
}

SerdStatus
serd_byte_source_prepare(SerdByteSource* const source)
{
  if (source->page_size == 0) {
    return SERD_FAILURE;
  }

  source->prepared = true;

  if (source->from_stream) {
    return (source->page_size > 1 ? serd_byte_source_page(source)
                                  : serd_byte_source_advance(source));
  }

  return SERD_SUCCESS;
}

SerdStatus
serd_byte_source_open_string(SerdByteSource* const source,
                             const uint8_t* const  utf8)
{
  const Cursor cur = {(const uint8_t*)"(string)", 1, 1};

  memset(source, '\0', sizeof(*source));
  source->page_size = 1;
  source->cur       = cur;
  source->read_buf  = utf8;
  return SERD_SUCCESS;
}

SerdStatus
serd_byte_source_close(SerdByteSource* const source)
{
  if (source->page_size > 1) {
    serd_free_aligned(source->file_buf);
  }

  memset(source, '\0', sizeof(*source));
  return SERD_SUCCESS;
}
