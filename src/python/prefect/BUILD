package(default_visibility = ["PUBLIC"])
python_library(
    name = "lib",
    srcs = glob(["**/*.py"], exclude=["**/*_test.py"]),
    resources = glob(["**/*.yml", "**/*.yaml"]),
    deps = [
        "//third_party/python:standard_deps",
        "//src/python/splitgraph:lib",
        ],
)

subinclude("//src/build/defs:build_ext")
python_test(
    name = "tests",
    srcs = glob(["**/*_test.py"]),
    resources = glob(["**/*.splitfile", "**/*.sgconfig"]),
    zip_safe = False,
    deps = [
        ":lib",
         "//third_party/python:pkg_resources",
        ],
)