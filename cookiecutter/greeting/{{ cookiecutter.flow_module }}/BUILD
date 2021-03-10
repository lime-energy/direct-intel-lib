python_library(
    name = "lib",
    srcs = glob(["**.*"]),
    deps = [
        "@lib//third_party/python:standard_deps"
    ],
)

python_binary(
    name = "{{ cookiecutter.flow_module }}",
    main = "flow.py",
    deps = [
        ":lib"
    ],
    visibility = ["PUBLIC"],
    labels = ["flow_reg"]
)
