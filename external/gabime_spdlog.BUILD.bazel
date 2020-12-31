load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "gabime_spdlog",
    hdrs = glob(["**/*.h"]),
    visibility = ["//visibility:public"],
    copts = ["-Iexternal/gabime_spdlog"],
    includes = ["./"]
)
