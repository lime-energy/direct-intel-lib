package(default_visibility = ["PUBLIC"])

subinclude("@pleasings//docker")


python_binary(
    name = "standard_deps_bin",
    main = "noop.py",
    deps = [
        "//dilib/prefect:lib",
        "//third_party/python:standard_deps",
        ],
)

docker_image(
    name = "docker_img",
    repo = "docker.pkg.github.com",
    image = "prefect-dilib-standard",
    dockerfile = "Dockerfile",
    version = "latest",
    srcs = [":standard_deps_bin"],
)
