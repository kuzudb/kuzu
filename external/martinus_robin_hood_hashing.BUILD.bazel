load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "martinus_robin_hood_hashing",
    hdrs = glob(["*.h"]),
    copts = ["-Iexternal/martinus_robin_hood_hashing"],
    includes = ["./"],
    visibility = ["//visibility:public"],
)
