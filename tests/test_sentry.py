import os

import requests_mock
import simplejson
import unittest
from singer import Schema

from tap_sentry import SentryAuthentication, SentryClient, SentrySync
import asyncio
import mock


def load_file_current(filename, path):
    myDir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(myDir, path, filename)
    with open(path) as file:
        return simplejson.load(file)

def load_file(filename, path):
    sibB = os.path.join(os.path.dirname(__file__), '..', path)
    with open(os.path.join(sibB, filename)) as f:
        return simplejson.load(f)

# Our test case class
class MyGreatClassTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(MyGreatClassTestCase, self).__init__(*args, **kwargs)
        self.client = SentryClient(SentryAuthentication("111"))

    @requests_mock.mock()
    def test_projects(self, m):
        record_value = load_file_current('projects_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/projects/', json=[record_value])
        self.assertEqual(self.client.projects(), [record_value])

    @requests_mock.mock()
    def test_sync_projects(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('projects_output.json', 'data_test')
        with mock.patch.object(SentryClient, 'projects', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('projects.json', 'tap_sentry/schemas')
            resp = dataSync.sync_projects(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_once_with('projects', record_value)

    @requests_mock.mock()
    def test_events(self, m):
        record_value = load_file_current('events_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/events/?project=1', json=[record_value])
        self.assertEqual(self.client.events(1, {}), [record_value])

    @requests_mock.mock()
    def test_sync_events(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('events_output.json', 'data_test')
        with mock.patch.object(SentryClient, 'projects', return_value=[{"id":1}]):
            with mock.patch('tap_sentry.SentryClient.events', return_value=[record_value]):
                dataSync = SentrySync(self.client)
                schema = load_file('events.json', 'tap_sentry/schemas')
                resp = dataSync.sync_events(Schema(schema))
                with mock.patch('singer.write_record') as patching:
                    task = asyncio.gather(resp)
                    loop.run_until_complete(task)
                    patching.assert_called_with('events', record_value)

    @requests_mock.mock()
    def test_issues(self, m):
        record_value = load_file_current('issues_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/issues/?project=1', json=[record_value])
        self.assertEqual(self.client.issues(1, {}), [record_value])

    @requests_mock.mock()
    def test_sync_issues(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('issues_output.json', 'data_test')
        with mock.patch.object(SentryClient, 'projects', return_value=[{"id":1}]):
            with mock.patch('tap_sentry.SentryClient.issues', return_value=[record_value]):
                dataSync = SentrySync(self.client)
                schema = load_file('issues.json', 'tap_sentry/schemas')
                resp = dataSync.sync_issues(Schema(schema))
                with mock.patch('singer.write_record') as patching:
                    task = asyncio.gather(resp)
                    loop.run_until_complete(task)
                    patching.assert_called_with('issues', record_value)

    @requests_mock.mock()
    def test_teams(self, m):
        record_value = load_file_current('teams_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/teams/', json=[record_value])
        self.assertEqual(self.client.teams({}), [record_value])

    @requests_mock.mock()
    def test_sync_teams(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('teams_output.json', 'data_test')
        with mock.patch('tap_sentry.SentryClient.teams', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('teams.json', 'tap_sentry/schemas')
            resp = dataSync.sync_teams(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_with('teams', record_value)

    @requests_mock.mock()
    def test_users(self, m):
        record_value = load_file_current('users_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/users/', json=[record_value])
        self.assertEqual(self.client.users({}), [record_value])

    @requests_mock.mock()
    def test_sync_users(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('teams_output.json', 'data_test')
        with mock.patch('tap_sentry.SentryClient.users', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('users.json', 'tap_sentry/schemas')
            resp = dataSync.sync_users(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_with('users', record_value)


if __name__ == '__main__':
    unittest.main()