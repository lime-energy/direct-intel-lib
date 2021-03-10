## DI Lib -  Cookiecutter

Set of cookiecuter projects with Prefect flow boilerplates. Can be used when starting a new Prefect flow.

The projects use [plz](https://please.build/) to manage build and run.

## 1 Requirements
- [Cookiecutter](https://cookiecutter.readthedocs.io/en/1.7.2/installation.html)

## 2 Use

```bash
cookiecutter git@github.com:lime-energy/direct-intel-lib.git --checkout "<branch>" --directory "<dir_name>"
```

## 3 Example Flows

### 3.1 Simple Greeting

```bash
cookiecutter git@github.com:lime-energy/direct-intel-lib.git --checkout "dev/cookiecutter" --directory "cookiecutter/greeting"
```


