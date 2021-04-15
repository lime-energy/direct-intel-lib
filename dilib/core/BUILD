package(default_visibility = ["PUBLIC"])
python_library(
    name = "lib",
    srcs = glob(["**.py"], exclude=["*_test.py"]),
    resources = glob(["**/*.yml", "**/*.yaml"]),
    deps = [
        # "//third_party/python:standard_deps",
    ],
)



python_test(
    name = "tests",
    srcs = glob(["**/*_test.py"]),
    resources = glob(["**/*.yml", "**/*.yaml"]),
    site = True,
    zip_safe = False,
    data = [
        "//fake_proj:test_proj_config",
    ],
    deps = [
        ":lib",
    ],
)