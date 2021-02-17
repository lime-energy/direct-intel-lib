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
  package_name = "prefect[sql_server]",
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
