package(default_visibility = ["PUBLIC"])
python_library(
    name = "lib",
    srcs = glob(["**.py"], exclude=["*_test.py"]),
    deps = [
        # "//third_party/python:standard_deps",
    ],
)



python_test(
    name = "tests",
    srcs = glob(["**/*_test.py"]),
    site = True,
    deps = [
        ":lib",
        ],
)