package(default_visibility = ["PUBLIC"])


filegroup(
  name = "standard_deps",
   deps = [
        ":prefect",
        ":splitgraph",
        ":pandas"
      ]
)




# ██████╗ ██████╗ ███████╗███████╗███████╗ ██████╗████████╗
# ██╔══██╗██╔══██╗██╔════╝██╔════╝██╔════╝██╔════╝╚══██╔══╝
# ██████╔╝██████╔╝█████╗  █████╗  █████╗  ██║        ██║
# ██╔═══╝ ██╔══██╗██╔══╝  ██╔══╝  ██╔══╝  ██║        ██║
# ██║     ██║  ██║███████╗██║     ███████╗╚██████╗   ██║
# ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝ ╚═════╝   ╚═╝
#
pip_library(
  name = "prefect",
  package_name = "prefect[aws,sql_server]",
  version = "0.14.8",
  deps = [
    ":toml",
    ":python-box",
    ":toolz",
    ":marshmallow-oneofschema",
    ":marshmallow",
    ":cloudpickle",
    ":python-slugify",
    ":text-unidecode",
    ":PyYAML",
    ":pytz",
    ":croniter",
    ":python-dateutil",
    ":six",
    ":natsort",
    ":mypy-extensions",
    ":requests",
    ":urllib3",
    ":chardet",
    ":certifi",
    ":idna",
    ":tornado",
    ":pendulum",
    ":pytzdata",
    ":docker",
    ":websocket-client",
    ":dask",
    ":pyodbc",
    ":boto3",
    ":botocore",
    ":distributed",
    ":HeapDict",
    ":jmespath",
    ":msgpack",
    ":psutil",
    ":s3transfer",
    ":sortedcontainers",
    ":tabulate",
    ":tblib",
    ":zict",
  ]
)

pip_library(
  name = "python-box",
  version = "5.2.0",
)
pip_library(
  name = "pendulum",
  version = "2.1.2",
)
pip_library(
  name = "toml",
  version = "0.10.2",
  zip_safe = False,
)
pip_library(
  name = "toolz",
  version = "0.11.1",
)
pip_library(
  name = "marshmallow-oneofschema",
  version = "2.1.0",
)
pip_library(
  name = "marshmallow",
  version = "3.10.0",
)
pip_library(
  name = "cloudpickle",
  version = "1.6.0",
)

pip_library(
  name = "python-slugify",
  version = "4.0.1",
)
pip_library(
  name = "text-unidecode",
  version = "1.3",
)

pip_library(
  name = "PyYAML",
  version = "5.4.1",
)
pip_library(
  name = "pytz",
  version = "2021.1",
)

pip_library(
  name = "croniter",
  version = "0.3.37",
)
pip_library(
  name = "python-dateutil",
  version = "2.8.1",
)
pip_library(
  name = "six",
  version = "1.15.0",
)
pip_library(
  name = "natsort",
  version = "7.1.1",
)
pip_library(
  name = "mypy-extensions",
  version = "0.4.3",
)
pip_library(
  name = "requests",
  version = "2.25.1",
)
pip_library(
  name = "urllib3",
  version = "1.26.3",
)
pip_library(
  name = "chardet",
  version = "4.0.0",
)
pip_library(
  name = "certifi",
  version = "2020.12.5",
)
pip_library(
  name = "idna",
  version = "2.10",
)
pip_library(
  name = "tornado",
  version = "6.1",
)
pip_library(
  name = "pytzdata",
  version = "2020.1",
)
pip_library(
  name = "docker",
  version = "4.4.1",
)
pip_library(
  name = "websocket-client",
  version = "0.57.0",
)

pip_library(
  name = "dask",
  version = "2021.2.0",
)


pip_library(
  name = "pyodbc",
  version = "4.0.30",
)

pip_library(
  name = "boto3",
  version = "1.17.11",
)
pip_library(
  name = "botocore",
  version = "1.20.11",
)
pip_library(
  name ="distributed",
  version = "2021.2.0",
)
pip_library(
  name ="HeapDict",
  version = "1.0.1",
)
pip_library(
  name ="jmespath",
  version = "0.10.0",
)
pip_library(
  name ="msgpack",
  version = "1.0.2",
)
pip_library(
  name ="psutil",
  version = "5.8.0",
)
pip_library(
  name ="s3transfer",
  version = "0.3.4",
)
pip_library(
  name ="sortedcontainers",
  version = "2.3.0",
)
pip_library(
  name ="tabulate",
  version = "0.8.8",
)
pip_library(
  name ="tblib",
  version = "1.7.0",
)
pip_library(
  name ="zict",
  version = "2.0.0",
)


#     ██╗ ██╗    ██████╗ ██████╗ ███████╗███████╗███████╗ ██████╗████████╗
#    ██╔╝██╔╝    ██╔══██╗██╔══██╗██╔════╝██╔════╝██╔════╝██╔════╝╚══██╔══╝
#   ██╔╝██╔╝     ██████╔╝██████╔╝█████╗  █████╗  █████╗  ██║        ██║
#  ██╔╝██╔╝      ██╔═══╝ ██╔══██╗██╔══╝  ██╔══╝  ██╔══╝  ██║        ██║
# ██╔╝██╔╝       ██║     ██║  ██║███████╗██║     ███████╗╚██████╗   ██║
# ╚═╝ ╚═╝        ╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝ ╚═════╝   ╚═╝
#



# ██████╗  █████╗ ███╗   ██╗██████╗  █████╗ ███████╗
# ██╔══██╗██╔══██╗████╗  ██║██╔══██╗██╔══██╗██╔════╝
# ██████╔╝███████║██╔██╗ ██║██║  ██║███████║███████╗
# ██╔═══╝ ██╔══██║██║╚██╗██║██║  ██║██╔══██║╚════██║
# ██║     ██║  ██║██║ ╚████║██████╔╝██║  ██║███████║
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═╝  ╚═╝╚══════╝
#
pip_library(
  name = "pandas",
  version = "1.2.2",
  deps = [
    ":numpy",
  ]
)

pip_library(
  name = "numpy",
  version = "1.20.1",
)


#     ██╗ ██╗    ██████╗  █████╗ ███╗   ██╗██████╗  █████╗ ███████╗
#    ██╔╝██╔╝    ██╔══██╗██╔══██╗████╗  ██║██╔══██╗██╔══██╗██╔════╝
#   ██╔╝██╔╝     ██████╔╝███████║██╔██╗ ██║██║  ██║███████║███████╗
#  ██╔╝██╔╝      ██╔═══╝ ██╔══██║██║╚██╗██║██║  ██║██╔══██║╚════██║
# ██╔╝██╔╝       ██║     ██║  ██║██║ ╚████║██████╔╝██║  ██║███████║
# ╚═╝ ╚═╝        ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═╝  ╚═╝╚══════╝
#


pip_library(
  name = "splitgraph",
  version = "0.2.5",
  deps = [
    ":psycopg2-binary",
    ":tqdm",
    ":asciitree",
    ":attrs",
    ":packaging",
    ":splitgraph-pipelinewise-target-postgres",
    ":sqlalchemy",
  ]
)

pip_library(
  name = "psycopg2-binary",
  version = "2.8.6",
)

pip_library(
  name = "tqdm",
  version = "4.56.2",
)
pip_library(
  name = "asciitree",
  version = "0.3.3",
)
pip_library(
  name = "attrs",
  version = "20.3.0",
)
pip_library(
  name = "packaging",
  version = "20.9",
)
pip_library(
  name = "splitgraph-pipelinewise-target-postgres",
  version = "2.1.0",
)
pip_library(
  name = "sqlalchemy",
  version = "1.3.22",
)

# certifi==2020.12.5
# chardet==4.0.0
# click==7.1.2
# click-log==0.3.2
# docker==4.4.2
# idna==2.10
# inflection==0.5.1
# joblib==1.0.1
# jsonschema==3.2.0
# minio==7.0.2

# parsimonious==0.8.1
# pglast==1.14
# psycopg2-binary==2.8.6
# pyparsing==2.4.7
# pyrsistent==0.17.3
# PyYAML==5.4.1
# requests==2.25.1
# six==1.15.0
# sodapy==2.1.0

# tabulate==0.8.7
# urllib3==1.26.3
# websocket-client==0.57.0