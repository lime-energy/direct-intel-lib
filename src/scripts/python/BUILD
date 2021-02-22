filegroup(
    name = "requirements",
    srcs = ["requirements.build_defs"],
    visibility = ["PUBLIC"],
)


subinclude(":requirements")

generate_requirements(
    name = "venv_requirements"
)

sh_cmd(
    name = "build_venv",
    cmd = [
        "PYTHON_BIN=python3",
        "VIRTUALENV=.venv",
        "rm -rf \\\\${VIRTUALENV}",
        "PIP=\"\\\\${VIRTUALENV}/bin/pip\"",
        "REQUIREMENTS_FILE=$(out_location :venv_requirements)",
        "echo \\\\$REQUIREMENTS_FILE",
        "\"\\\\${PYTHON_BIN}\" -m venv \"\\\\${VIRTUALENV}\"",
        "\"\\\\${PIP}\" install pip --upgrade",
        "\"\\\\${PIP}\" install -r \"\\\\${REQUIREMENTS_FILE}\"",
    ],
    deps = [
        ":venv_requirements",
    ],
)