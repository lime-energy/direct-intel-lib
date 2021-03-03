package(default_visibility = ["PUBLIC"])
python_library(
    name = "lib",
    srcs = glob(["**/*.py"], exclude=["**/*_test.py"]),
    resources = glob(["**/*.yml", "**/*.yaml"]),
    deps = [
        # "//third_party/python:standard_deps",
        "//dilib/splitgraph:lib",
        ],
)

# subinclude("//src/build/defs:build_ext")
python_test(
    name = "tests",
    srcs = glob(["**/*_test.py"]),
    resources = glob(["**/*.splitfile"]),
    #zip_safe = False,
    site = True,
    deps = [
        ":lib",

        ],
)

