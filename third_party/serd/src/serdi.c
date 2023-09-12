// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#include "serd_config.h"
#include "string_utils.h"

#include "serd.h"

#ifdef _WIN32
#  ifdef _MSC_VER
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <fcntl.h>
#  include <io.h>
#endif

#if USE_POSIX_FADVISE && USE_FILENO
#  include <fcntl.h>
#endif

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SERDI_ERROR(msg) fprintf(stderr, "serdi: " msg)
#define SERDI_ERRORF(fmt, ...) fprintf(stderr, "serdi: " fmt, __VA_ARGS__)

typedef struct {
  SerdSyntax  syntax;
  const char* name;
  const char* extension;
} Syntax;

static const Syntax syntaxes[] = {{SERD_TURTLE, "turtle", ".ttl"},
                                  {SERD_NTRIPLES, "ntriples", ".nt"},
                                  {SERD_NQUADS, "nquads", ".nq"},
                                  {SERD_TRIG, "trig", ".trig"},
                                  {(SerdSyntax)0, NULL, NULL}};

static SerdSyntax
get_syntax(const char* const name)
{
  for (const Syntax* s = syntaxes; s->name; ++s) {
    if (!serd_strncasecmp(s->name, name, strlen(name))) {
      return s->syntax;
    }
  }

  SERDI_ERRORF("unknown syntax '%s'\n", name);
  return (SerdSyntax)0;
}

static SERD_PURE_FUNC SerdSyntax
guess_syntax(const char* const filename)
{
  const char* ext = strrchr(filename, '.');
  if (ext) {
    for (const Syntax* s = syntaxes; s->name; ++s) {
      if (!serd_strncasecmp(s->extension, ext, strlen(ext))) {
        return s->syntax;
      }
    }
  }

  return (SerdSyntax)0;
}

static int
print_version(void)
{
  printf("serdi " SERD_VERSION " <http://drobilla.net/software/serd>\n");
  printf("Copyright 2011-2023 David Robillard <d@drobilla.net>.\n"
         "License ISC: <https://spdx.org/licenses/ISC>.\n"
         "This is free software; you are free to change and redistribute it."
         "\nThere is NO WARRANTY, to the extent permitted by law.\n");
  return 0;
}

static int
print_usage(const char* const name, const bool error)
{
  static const char* const description =
    "Read and write RDF syntax.\n"
    "Use - for INPUT to read from standard input.\n\n"
    "  -a           Write ASCII output.\n"
    "  -b           Write output in blocks for performance.\n"
    "  -c PREFIX    Chop PREFIX from matching blank node IDs.\n"
    "  -e           Eat input one character at a time.\n"
    "  -f           Fast and loose URI pass-through.\n"
    "  -h           Display this help and exit.\n"
    "  -i SYNTAX    Input syntax: turtle/ntriples/trig/nquads.\n"
    "  -l           Lax (non-strict) parsing.\n"
    "  -o SYNTAX    Output syntax: turtle/ntriples/nquads.\n"
    "  -p PREFIX    Add PREFIX to blank node IDs.\n"
    "  -q           Suppress all output except data.\n"
    "  -r ROOT_URI  Keep relative URIs within ROOT_URI.\n"
    "  -s INPUT     Parse INPUT as string (terminates options).\n"
    "  -v           Display version information and exit.\n";

  FILE* const os = error ? stderr : stdout;
  fprintf(os, "%s", error ? "\n" : "");
  fprintf(os, "Usage: %s [OPTION]... INPUT [BASE_URI]\n", name);
  fprintf(os, "%s", description);
  return error ? 1 : 0;
}

static int
missing_arg(const char* const name, const char opt)
{
  SERDI_ERRORF("option requires an argument -- '%c'\n", opt);
  return print_usage(name, true);
}

static SerdStatus
quiet_error_sink(void* const handle, const SerdError* const e)
{
  (void)handle;
  (void)e;
  return SERD_SUCCESS;
}

static FILE*
serd_fopen(const char* const path, const char* const mode)
{
  FILE* fd = fopen(path, mode);
  if (!fd) {
    SERDI_ERRORF("failed to open file %s (%s)\n", path, strerror(errno));
    return NULL;
  }

#if USE_POSIX_FADVISE && USE_FILENO
  (void)posix_fadvise(
    fileno(fd), 0, 0, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE);
#endif

  return fd;
}

static SerdStyle
choose_style(const SerdSyntax input_syntax,
             const SerdSyntax output_syntax,
             const bool       ascii,
             const bool       bulk_write,
             const bool       full_uris,
             const bool       lax)
{
  unsigned output_style = 0U;
  if (output_syntax == SERD_NTRIPLES || ascii) {
    output_style |= SERD_STYLE_ASCII;
  } else if (output_syntax == SERD_TURTLE) {
    output_style |= SERD_STYLE_ABBREVIATED;
    if (!full_uris) {
      output_style |= SERD_STYLE_CURIED;
    }
  }

  if ((input_syntax == SERD_TURTLE || input_syntax == SERD_TRIG) ||
      (output_style & SERD_STYLE_CURIED)) {
    // Base URI may change and/or we're abbreviating URIs, so must resolve
    output_style |= SERD_STYLE_RESOLVED;
  }

  if (bulk_write) {
    output_style |= SERD_STYLE_BULK;
  }

  if (!lax) {
    output_style |= SERD_STYLE_STRICT;
  }

  return (SerdStyle)output_style;
}

int
main(int argc, char** argv)
{
  const char* const prog = argv[0];

  FILE*          in_fd         = NULL;
  SerdSyntax     input_syntax  = (SerdSyntax)0;
  SerdSyntax     output_syntax = (SerdSyntax)0;
  bool           from_file     = true;
  bool           ascii         = false;
  bool           bulk_read     = true;
  bool           bulk_write    = false;
  bool           full_uris     = false;
  bool           lax           = false;
  bool           quiet         = false;
  const uint8_t* in_name       = NULL;
  const uint8_t* add_prefix    = NULL;
  const uint8_t* chop_prefix   = NULL;
  const uint8_t* root_uri      = NULL;
  int            a             = 1;
  for (; a < argc && from_file && argv[a][0] == '-'; ++a) {
    if (argv[a][1] == '\0') {
      in_name = (const uint8_t*)"(stdin)";
      in_fd   = stdin;
      break;
    }

    if (!strcmp(argv[a], "--help")) {
      return print_usage(prog, false);
    }

    if (!strcmp(argv[a], "--version")) {
      return print_version();
    }

    for (int o = 1; argv[a][o]; ++o) {
      const char opt = argv[a][o];

      if (opt == 'a') {
        ascii = true;
      } else if (opt == 'b') {
        bulk_write = true;
      } else if (opt == 'e') {
        bulk_read = false;
      } else if (opt == 'f') {
        full_uris = true;
      } else if (opt == 'h') {
        return print_usage(prog, false);
      } else if (opt == 'l') {
        lax = true;
      } else if (opt == 'q') {
        quiet = true;
      } else if (opt == 'v') {
        return print_version();
      } else if (opt == 's') {
        in_name   = (const uint8_t*)"(string)";
        from_file = false;
        break;
      } else if (opt == 'c') {
        if (argv[a][o + 1] || ++a == argc) {
          return missing_arg(prog, 'c');
        }

        chop_prefix = (const uint8_t*)argv[a];
        break;
      } else if (opt == 'i') {
        if (argv[a][o + 1] || ++a == argc) {
          return missing_arg(prog, 'i');
        }

        if (!(input_syntax = get_syntax(argv[a]))) {
          return print_usage(prog, true);
        }
        break;
      } else if (opt == 'o') {
        if (argv[a][o + 1] || ++a == argc) {
          return missing_arg(prog, 'o');
        }

        if (!(output_syntax = get_syntax(argv[a]))) {
          return print_usage(prog, true);
        }
        break;
      } else if (opt == 'p') {
        if (argv[a][o + 1] || ++a == argc) {
          return missing_arg(prog, 'p');
        }

        add_prefix = (const uint8_t*)argv[a];
        break;
      } else if (opt == 'r') {
        if (argv[a][o + 1] || ++a == argc) {
          return missing_arg(prog, 'r');
        }

        root_uri = (const uint8_t*)argv[a];
        break;
      } else {
        SERDI_ERRORF("invalid option -- '%s'\n", argv[a] + 1);
        return print_usage(prog, true);
      }
    }
  }

  if (a == argc) {
    SERDI_ERROR("missing input\n");
    return print_usage(prog, true);
  }

#ifdef _WIN32
  _setmode(_fileno(stdin), _O_BINARY);
  _setmode(_fileno(stdout), _O_BINARY);
#endif

  uint8_t*       input_path = NULL;
  const uint8_t* input      = (const uint8_t*)argv[a++];
  if (from_file) {
    in_name = in_name ? in_name : input;
    if (!in_fd) {
      if (!strncmp((const char*)input, "file:", 5)) {
        input_path = serd_file_uri_parse(input, NULL);
        input      = input_path;
      }
      if (!input || !(in_fd = serd_fopen((const char*)input, "rb"))) {
        return 1;
      }
    }
  }

  if (!input_syntax && !(input_syntax = guess_syntax((const char*)in_name))) {
    input_syntax = SERD_TRIG;
  }

  if (!output_syntax) {
    output_syntax =
      ((input_syntax == SERD_TURTLE || input_syntax == SERD_NTRIPLES)
         ? SERD_NTRIPLES
         : SERD_NQUADS);
  }

  const SerdStyle output_style = choose_style(
    input_syntax, output_syntax, ascii, bulk_write, full_uris, lax);

  SerdURI  base_uri = SERD_URI_NULL;
  SerdNode base     = SERD_NODE_NULL;
  if (a < argc) { // Base URI given on command line
    base =
      serd_node_new_uri_from_string((const uint8_t*)argv[a], NULL, &base_uri);
  } else if (from_file && in_fd != stdin) { // Use input file URI
    base = serd_node_new_file_uri(input, NULL, &base_uri, true);
  }

  FILE* const    out_fd = stdout;
  SerdEnv* const env    = serd_env_new(&base);

  SerdWriter* const writer = serd_writer_new(
    output_syntax, output_style, env, &base_uri, serd_file_sink, out_fd);

  SerdReader* const reader =
    serd_reader_new(input_syntax,
                    writer,
                    NULL,
                    (SerdBaseSink)serd_writer_set_base_uri,
                    (SerdPrefixSink)serd_writer_set_prefix,
                    (SerdStatementSink)serd_writer_write_statement,
                    (SerdEndSink)serd_writer_end_anon);

  serd_reader_set_strict(reader, !lax);
  if (quiet) {
    serd_reader_set_error_sink(reader, quiet_error_sink, NULL);
    serd_writer_set_error_sink(writer, quiet_error_sink, NULL);
  }

  SerdNode root = serd_node_from_string(SERD_URI, root_uri);
  serd_writer_set_root_uri(writer, &root);
  serd_writer_chop_blank_prefix(writer, chop_prefix);
  serd_reader_add_blank_prefix(reader, add_prefix);

  SerdStatus st = SERD_SUCCESS;
  if (!from_file) {
    st = serd_reader_read_string(reader, input);
  } else if (bulk_read) {
    st = serd_reader_read_file_handle(reader, in_fd, in_name);
  } else {
    st = serd_reader_start_stream(reader, in_fd, in_name, false);
    while (!st) {
      st = serd_reader_read_chunk(reader);
    }
    serd_reader_end_stream(reader);
  }

  serd_reader_free(reader);
  serd_writer_finish(writer);
  serd_writer_free(writer);
  serd_env_free(env);
  serd_node_free(&base);
  free(input_path);

  if (from_file) {
    fclose(in_fd);
  }

  if (fclose(out_fd)) {
    perror("serdi: write error");
    st = SERD_ERR_UNKNOWN;
  }

  return (st > SERD_FAILURE) ? 1 : 0;
}
