import pkgutil
import unittest
from typing import List

import numpy as np
import pandas as pd
import prefect
from dilib.prefect.tasks.splitgraph import (CommitTask, DataFrameToTableParams,
                                            DataFrameToTableTask, PushRepoTask,
                                            SemanticBumpTask,
                                            SemanticCheckoutTask)
from dilib.splitgraph import RepoInfo
from prefect import Task
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.results.constant_result import ConstantResult
from prefect.engine.state import Success
from prefect.tasks.core.constants import Constant
from prefect.utilities.debug import raise_on_exception
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

    def test_can_checkout_new_tag(self):
        repo_info = RepoInfo(namespace="abc", repository="1234")

        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
            major="1",
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()


                image_hash, version = state.result
                self.assertEqual(version, None)

    def test_checkout_new_prerelease_fails(self):
        repo_info = RepoInfo(namespace="abc", repository="1234")

        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
            major="1",
            prerelease="hourly"
        )
        runner = TaskRunner(task=checkout)

        with prefect.context():
            state = runner.run()
            self.assertTrue(state.is_failed(), 'A repo must first be initialized with a non-prerelease tag.')

    def test_can_checkout_already_tagged_repo(self):
        self.tag_repo(['1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory'])
        repo_info = RepoInfo(namespace="abc", repository="1234")

        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
            major="1",
        )
        runner = TaskRunner(task=checkout)

        with raise_on_exception():
            with prefect.context():
                state = runner.run()

                if state.is_failed():
                    print(state)
                    self.fail()

                image_hash, version = state.result
                self.assertEqual(version, Version('1.0.0'))

    def test_can_clone_repo_with_patches(self):
        self.tag_repo(['1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory', '1.0.1', '1.0.1+20200307.pink-bear'])
        repo_info = RepoInfo(namespace="abc", repository="1234")

        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
            major="1",
            minor="1",
        )
        runner = TaskRunner(task=checkout)

        with prefect.context():
            state = runner.run()

            if state.is_failed():
                print(state)
                self.fail()

            image_hash, version = state.result
            self.assertEqual(version, Version('1.0.1'))


    def test_can_clone_with_hourly_prerelease_tags(self):
        self.tag_repo([
            '1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory',
            '1.0.1', '1.0.1+20200307.pink-bear', '1-hourly',
            '1.0-hourly',
            '1.0.2-hourly.1+20200301.blue-ivory',
            '1.0.2-hourly.2+20200301.green-monster']
        )
        repo_info = RepoInfo(namespace="abc", repository="1234")

        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
            major="1",
            minor="0",
            prerelease="hourly"
        )
        runner = TaskRunner(task=checkout)

        with prefect.context():
            state = runner.run()

            if state.is_failed():
                print(state)
                self.fail()

            image_hash, version = state.result
            self.assertEqual(version, Version('1.0.2-hourly.2+20200301.green-monster'))

    def test_can_commit(self):
        repo_info = RepoInfo(namespace="abc", repository="1234")


        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
        )
        commit = CommitTask(
            repo_info=repo_info,
        )
        checkout.run()
        df_to_table(fake_data(10), repository=self.repo, table="unit_test", if_exists='replace')

        runner = TaskRunner(task=commit)


        with prefect.context():
            state = runner.run()

            if state.is_failed():
                print(state)
                self.fail()
        self.assertListEqual(self.repo.head.get_tags(), ['HEAD'])

    def test_can_commit_with_tags(self):
        self.tag_repo(['1.0.0', '1', '1.0', '1.0.0+20200228.blue-ivory'])
        repo_info = RepoInfo(namespace="abc", repository="1234")


        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
        )
        commit = CommitTask(
            repo_info=repo_info,
        )
        checkout.run()
        df_to_table(fake_data(10), repository=self.repo, table="unit_test", if_exists='replace')


        runner = TaskRunner(task=commit)
        upstream_edge = Edge(Task(), commit, key='tags')
        tag_state = Success(result=ConstantResult(value=['foo', 'bar', 'tag1_w_upstream']))

        with prefect.context():
            state = runner.run(upstream_states={upstream_edge: tag_state})

            if state.is_failed():
                print(state)
                self.fail()

            self.assertListEqual(self.repo.head.get_tags(), ['HEAD', 'foo', 'bar', 'tag1_w_upstream'])
    def test_can_import_df(self):
        repo_info = RepoInfo(namespace="abc", repository="1234")


        checkout = SemanticCheckoutTask(
            repo_info=repo_info,
        )
        checkout.run()
        df_to_table = DataFrameToTableTask(
            repo_info=repo_info,
            table='footable1'
        )

        runner = TaskRunner(task=df_to_table)
        df_edge = Edge(Task(), df_to_table, key='input')
        upstream_state = Success(result=ConstantResult(value=DataFrameToTableParams(fake_data(10))))

        with prefect.context():
            state = runner.run(upstream_states={df_edge: upstream_state})

            if state.is_failed():
                print(state)
                self.fail()

            self.assertTrue(table_exists_at(self.repo, 'footable1'))

    def test_can_semantic_bump(self):

        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='base_ref')
        upstream_state = Success(result=ConstantResult(value=Version('1.0.0')))

        with raise_on_exception():
            with prefect.context(today='day1', task_run_id='123'):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, ['1', '1.0', '1.0.1+day1.123'])

    def test_can_semantic_bump_init_repo(self):

        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='base_ref')
        upstream_state = Success(result=ConstantResult(value=None))

        with raise_on_exception():
            with prefect.context(today='day1', task_run_id='123'):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, ['1', '1.0', '1.0.0+day1.123'])

    def test_can_semantic_bump_prerelease(self):

        semantic_bump = SemanticBumpTask()

        runner = TaskRunner(task=semantic_bump)
        edge = Edge(Task(), semantic_bump, key='base_ref')
        upstream_state = Success(result=ConstantResult(value=Version('1.0.1-hourly.4+2021-03-08.zip-fur')))

        with raise_on_exception():
            with prefect.context(today='day1', task_run_id='123'):
                state = runner.run(upstream_states={edge: upstream_state})

                if state.is_failed():
                    print(state)
                    self.fail()


                self.assertEqual(state.result, ['1-hourly', '1.0-hourly', '1.0.1-hourly.5+day1.123'])

    def test_can_push(self):
        repo_info = RepoInfo(namespace="abc", repository="1234")

        push = PushRepoTask(
            repo_info=repo_info,
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
