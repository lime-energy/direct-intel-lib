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
        "BASE_PYTHON=python3",
        "VIRTUALENV=.venv",
        "rm -rf \\\\${VIRTUALENV}",
        "PIP=\"\\\\${VIRTUALENV}/bin/pip\"",
        "VENV_PYTHON=\"\\\\${VIRTUALENV}/bin/python\"",
        "REQUIREMENTS_FILE=$(out_location :venv)",
        "echo \\\\$REQUIREMENTS_FILE",
        "\"\\\\${BASE_PYTHON}\" -m venv \"\\\\${VIRTUALENV}\"",
        "\"\\\\${PIP}\" install pip --upgrade",
        "\"\\\\${PIP}\" install -r \"\\\\${REQUIREMENTS_FILE}\"",
        "cp -r plz-out/gen/*/src \\\\$(\\\\${VENV_PYTHON} -c 'import site; print(site.getsitepackages()[0])')"
    ],
    deps = [
        ":venv_requirements",
    ],
)
