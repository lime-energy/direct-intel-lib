
import pkgutil
import unittest
from datetime import datetime
from typing import List, TypedDict

import numpy as np
import pandas as pd
import pendulum
import prefect
from dilib.prefect.tasks.splitgraph import (CommitTask, DataFrameToTableParams,
                                            DataFrameToTableTask, PushRepoTask,
                                            SemanticBumpTask,
                                            SemanticCheckoutTask,
                                            SemanticCleanupTask,
                                            semantic_operation,
                                            VersionToDateTask, Workspace)
from dilib.splitgraph import RepoInfo
from prefect import Flow, Parameter, Task, apply_map, task
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.flow_runner import FlowRunner
from prefect.engine.results.constant_result import ConstantResult
from prefect.engine.state import Success
from prefect.tasks.core.constants import Constant
from prefect.utilities.debug import raise_on_exception
from prefect.utilities.edges import unmapped
from semantic_version import Version

from splitgraph.core.engine import repository_exists
from splitgraph.core.repository import Repository, table_exists_at
from splitgraph.ingestion.pandas import df_to_table


def fake_data(periods: int):
    index = pd.date_range("1/1/2000", periods=periods)
    s = pd.Series(np.random.randn(5), index=["a", "b", "c", "d", "e"])
    df = pd.DataFrame(np.random.randn(periods, 3), index=index, columns=["A", "B", "C"])

    return df
def build_repo():
    repo = Repository(namespace="abc", repository="1234")
    repo.delete()
    repo.init()
    df_to_table(fake_data(8), repository=repo, table="unit_test", if_exists='replace')
    new_img = repo.commit()
    new_img.checkout()

    return repo

@task
def say_something(msg: str):
    print(msg)
@task
def fake_extract(periods: int) -> DataFrameToTableParams:
    return DataFrameToTableParams(
        data_frame=fake_data(periods),
        table=f'periodic_data_{periods}',
        if_exists='replace'
    )

remote_name='bedrock'

df_to_table_task = DataFrameToTableTask()


with Flow('sample') as flow:
    extract_results = fake_extract.map(
        periods=[5,6,8],
    )

    upstream_repos = Parameter('upstream_repos')
    with semantic_operation(
        flow=flow,
        upstream_repos=upstream_repos, 
        versions_to_retain=Parameter('versions_to_retain'),
        remote_name=Parameter('remote_name')
    ) as op:
        # load_done = df_to_table_task(fake_extract(1), upstream_tasks=[base_ref])
        workspaces = op.workspaces
        foo1=workspaces['unittest']
        foo2=workspaces['foo']
        load_done = df_to_table_task.map(
            params=extract_results,
            repo_uri=unmapped(foo1['repo_uri'])
        )
        load_done2 = df_to_table_task.map(
            params=extract_results,
            repo_uri=unmapped(foo2['repo_uri'])
        )
        say_something('after_load', upstream_tasks=[load_done, load_done2])
    
   
    say_something('after with', upstream_tasks=[op.push])


class RepoTasksTest(unittest.TestCase):
    repo: Repository
    def setUp(self):
        self.repo = build_repo()

    def tearDown(self):
        self.repo.delete(unregister=True, uncheckout=True)

    def tag_repo(self, tags: List[str]):
        img = self.repo.head
        for tag in tags:
            img.tag(tag)


    def test_can_run_sample_flow(self):

        with raise_on_exception():
            with prefect.context(date=datetime.utcnow(), flow_run_name='foo1id'):
                state = flow.run(
                    parameters=dict(
                        remote_name=remote_name,
                        upstream_repos=dict(
                            unittest='foo13/unittest1:1.0',
                            foo='foo13/unittest2:1.0',
                        ),
                        versions_to_retain=5,
                    )
                )
   
                for task in flow.tasks:
                    print(f'{task.name} - {state.result[task]} - {state.result[task]._result.value}')

                if state.is_failed():
                    print(state)
                    print(state.result)
                    self.fail()
    def test_can_checkout_new_tag(self):

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()


                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], None)

    def test_checkout_new_prerelease_fails(self):
        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1-hourly',
            ),
        )
        runner = TaskRunner(task=checkout)

        with prefect.context():
            state = runner.run()
            self.assertTrue(state.is_failed(), 'A repo must first be initialized with a non-prerelease tag.')

    def test_can_checkout_already_tagged_repo(self):
        self.tag_repo(['1', '1.0', '1.0.0+20200228.blue-ivory', '1.0.1+20200228.silver-fish'])
  
        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], Version('1.0.1+20200228.silver-fish'))

   # In this case, we expect the matching major with the greatest minor
    def test_can_checkout_with_no_minor_specified(self):
        self.tag_repo(['1', '1.0', '1.1', '1.0.0+20200228.blue-ivory', '1.0.1+20200228.silver-fish', '1.1.1+20200228.blue-moon'])

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], Version('1.1.1+20200228.blue-moon'))

    def test_can_checkout_with_new_major(self):
        self.tag_repo(['1', '1.0', '1.1', '1.0.0+20200228.blue-ivory', '1.0.1+20200228.silver-fish', '1.1.1+20200228.blue-moon'])

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:2',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], Version('1.1.1+20200228.blue-moon'))

    def test_can_clone_repo_with_patches(self):
        self.tag_repo(['1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory', '1.0.1', '1.0.1+20200307.pink-bear'])

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1.1',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], Version('1.0.1+20200307.pink-bear'))


    def test_can_clone_with_hourly_prerelease_tags(self):
        self.tag_repo([
            '1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory',
            '1.0.1', '1.0.1+20200307.pink-bear', '1-hourly',
            '1.0-hourly',
            '1.0.2-hourly.1+20200301.blue-ivory',
            '1.0.2-hourly.2+20200301.green-monster']
        )

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1.1-hourly',
            ),
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

 
                workspaces = state.result
                workspace = workspaces['abc']
                self.assertEqual(workspace['version'], Version('1.0.2-hourly.2+20200301.green-monster'))

    def test_can_commit(self):


        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )
        workspaces = checkout.run()

        commit = CommitTask(
            workspaces=workspaces,
        )

        df_to_table(fake_data(10), repository=self.repo, table="unit_test", if_exists='replace')

        runner = TaskRunner(task=commit)
        with raise_on_exception():
            with prefect.context():
                state = runner.run()


                if state.is_failed():
                    print(state)
                    self.fail()
            self.assertListEqual(self.repo.head.get_tags(), ['HEAD'])

    def test_can_commit_with_tags(self):
        self.tag_repo(['1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory'])

        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )

        workspaces = checkout.run()
        commit = CommitTask(
            workspaces=workspaces,
        )
        df_to_table(fake_data(10), repository=self.repo, table="unit_test", if_exists='replace')


        runner = TaskRunner(task=commit)
        tags_edge = Edge(Task(), commit, key='sgr_tags')
        tag_state = Success(result=ConstantResult(value=dict(
            abc=['foo', 'bar', 'tag1_w_upstream']
        )))

        with raise_on_exception():
            with prefect.context():
                state = runner.run(upstream_states={tags_edge: tag_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertCountEqual(self.repo.head.get_tags(), ['HEAD', 'foo', 'bar', 'tag1_w_upstream'])

    def test_can_import_df(self):
        checkout = SemanticCheckoutTask(
            upstream_repos=dict(
                abc='abc/1234:1',
            ),
        )
        workspaces = checkout.run()
        df_to_table = DataFrameToTableTask(
            repo_uri='abc/1234:1',
        )

        runner = TaskRunner(task=df_to_table)
        df_edge = Edge(Task(), df_to_table, key='params')
        upstream_state = Success(result=ConstantResult(value=DataFrameToTableParams(data_frame=fake_data(10), table='footable1')))

        with raise_on_exception():
            with prefect.context():
                state = runner.run(upstream_states={df_edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()

                self.assertTrue(table_exists_at(self.repo, 'footable1'))

#     def test_version_to_date(self):

#         version_to_date = VersionToDateTask()

#         runner = TaskRunner(task=version_to_date)
#         edge = Edge(Task(), version_to_date, key='version')
#         upstream_state = Success(result=ConstantResult(value=Version('1.0.0+2021-03-03T00.stinky-fish')))


#         with raise_on_exception():
#             with prefect.context():
#                 state = runner.run(upstream_states={edge: upstream_state})

#                 if state.is_failed():
#                     print(state)
#                     self.fail()


#                 self.assertEqual(state.result, pendulum.parse('2021-03-03T00'))

    def test_can_semantic_bump(self):
        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='workspaces')
        upstream_state = Success(
            result=ConstantResult(
                value=dict(
                    abc=dict(
                        repo_uri='abc/1234:1',
                        version=Version('1.0.0+2021-03-03T00.stinky-fish'),                    
                    )
                )
            )
        )

        date = datetime.utcnow()
        flow_run_name = 'testflow1'
        with raise_on_exception():
            with prefect.context(date=date, flow_run_name=flow_run_name):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, dict(
                    abc=['1', '1.0', f'1.0.1+{date:%Y-%m-%dT%H}.{date:%M}.{flow_run_name}']
                ))

    def test_can_semantic_bump_init_repo(self):
        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='workspaces')
        upstream_state = Success(
            result=ConstantResult(
                value=dict(
                    abc=dict(
                        repo_uri='abc/1234:1',
                        version=None,                       
                    )
                )
            )
        )

        date = datetime.utcnow()
        flow_run_name = 'testflow1'
        with raise_on_exception():
            with prefect.context(date=date, flow_run_name=flow_run_name):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, dict(
                    abc=['1', '1.0', f'1.0.0+{date:%Y-%m-%dT%H}.{date:%M}.{flow_run_name}']
                ))

    def test_can_semantic_bump_new_major(self):

        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='workspaces')
        upstream_state = Success(
            result=ConstantResult(
                value=dict(
                    abc=dict(
                        repo_uri='abc/1234:2.1',
                        version=None,                     
                    )
                )
            )
        )

        date = datetime.utcnow()
        flow_run_name = 'testflow1'
        with raise_on_exception():
            with prefect.context(date=date, flow_run_name=flow_run_name):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, dict(
                    abc=['2', '2.1', f'2.1.0+{date:%Y-%m-%dT%H}.{date:%M}.{flow_run_name}']
                ))

    def test_can_semantic_bump_prerelease(self):
        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='workspaces')
        upstream_state = Success(
            result=ConstantResult(
                value=dict(
                    abc=dict(
                        repo_uri='abc/1234:1-hourly',
                        version=Version('1.0.1-hourly.4+2021-03-08.zip-fur'),                       
                    )
                )
            )
        )

        date = datetime.utcnow()
        flow_run_name = 'testflow1'
        with raise_on_exception():
            with prefect.context(date=date, flow_run_name=flow_run_name):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, dict(
                    abc=['1-hourly', '1.0-hourly', f'1.0.1-hourly.5+{date:%Y-%m-%dT%H}.{date:%M}.{flow_run_name}']
                ))

    def test_can_push(self):
        push = PushRepoTask(
            repo_uris=dict(
                abc='abc/1234:1',
            ),
            remote_name='bedrock'
        )

        df_to_table(fake_data(10), repository=self.repo, table="unit_test", if_exists='replace')
        self.repo.commit()
        self.repo.engine.commit()

        runner = TaskRunner(task=push)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()


if __name__ == '__main__':
    unittest.main()
