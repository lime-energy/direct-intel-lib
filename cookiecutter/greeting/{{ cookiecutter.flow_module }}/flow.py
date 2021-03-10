import argparse
import prefect
from prefect import task, Flow, Parameter

@task
def greeting(name: str) -> None:
    logger = prefect.context.get('logger')

    logger.info(f'Hello, {name}')


with Flow('{{ cookiecutter.flow_name }}') as flow:
    name = Parameter('name', default='World')

    greeting(name)

#--------------------------------------------------------------
# Flow arguments - No need to change what's bellow
# [Empty] to run locally
# --reg to register
#--------------------------------------------------------------

def _get_args():
    parser = argparse.ArgumentParser(description=(
        '{{ cookiecutter.description }}'
    ))

    parser.add_argument('--reg', help='Param description',
        action='store_const', const=True, default=False)

    return parser.parse_args()

def register():
    flow.register(
        project_name='{{ cookiecutter.project_name }}'
    )

def run():
    flow.run()

def main():
    args = _get_args()

    if args.reg:
        register()
    else:
        run()

if __name__ == '__main__':
    main()