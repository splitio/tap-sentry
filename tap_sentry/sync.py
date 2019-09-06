import os
import json
import asyncio
import urllib
from pathlib import Path
from itertools import repeat

from singer import Schema
from urllib.parse import urljoin

import pytz
import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark
from pendulum import datetime, period


class SentryAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class SentryClient:
    def __init__(self, auth: SentryAuthentication, url="https://sentry.io/api/0/"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        #url = urljoin(self._base_url, path)
        url = self._base_url + path
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response

    def projects(self):
        try:
            projects = self._get(f"/organizations/split-software/projects/")
            return projects.json()
        except:
            return None

    def issues(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "issues", "start")
            query = f"/organizations/split-software/issues/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            response = self._get(query)
            issues = response.json()
            url= response.url
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                issues += response.json()
            return issues

        except:
            return None

    def events(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "events", "start")
            query = f"/organizations/split-software/events/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            response = self._get(query)
            events = response.json()
            url= response.url
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                events += response.json()
            return events
        except:
            return None

    def teams(self, state):
        try:
            response = self._get(f"/organizations/split-software/teams/")
            teams = response.json()
            extraction_time = singer.utils.now()
            while (response.links is not None and response.links.__len__() >0  and  response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                teams += response.json()
            return teams
        except:
            return None

    def users(self, state):
        try:
            response = self._get(f"/organizations/split-software/users/")
            users = response.json()
            return users
        except:
            return None


class SentrySync:
    def __init__(self, client: SentryClient, state={}):
        self._client = client
        self._state = state
        self.projects = self.client.projects()

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    def sync(self, stream, schema):
        func = getattr(self, f"sync_{stream}")
        return func(schema)

    async def sync_issues(self, schema, period: pendulum.period = None):
        """Issues per project."""
        stream = "issues"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        extraction_time = singer.utils.now()
        if self.projects:
            for project in self.projects:
                issues = await loop.run_in_executor(None, self.client.issues, project['id'], self.state)
                if (issues):
                    for issue in issues:
                        singer.write_record(stream, issue)

        self.state = singer.write_bookmark(self.state, 'issues', 'start', singer.utils.strftime(extraction_time))

    async def sync_projects(self, schema):
        """Issues per project."""
        stream = "projects"
        loop = asyncio.get_event_loop()
        singer.write_schema('projects', schema.to_dict(), ["id"])
        if self.projects:
            for project in self.projects:
                singer.write_record(stream, project)


    async  def sync_events(self, schema, period: pendulum.period = None):
        """Events per project."""
        stream = "events"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["eventID"])
        extraction_time = singer.utils.now()
        if self.projects:
            for project in self.projects:
                events = await loop.run_in_executor(None, self.client.events, project['id'], self.state)
                if events:
                    for event in events:
                        singer.write_record(stream, event)
            self.state = singer.write_bookmark(self.state, 'events', 'start', singer.utils.strftime(extraction_time))

    async def sync_users(self, schema):
        "Users in the organization."
        stream = "users"
        loop = asyncio.get_event_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        users = await loop.run_in_executor(None, self.client.users, self.state)
        if users:
            for user in users:
                singer.write_record(stream, user)
        #extraction_time = singer.utils.now()
        #self.state = singer.write_bookmark(self.state, 'users', 'dateCreated', singer.utils.strftime(extraction_time))

    async def sync_teams(self, schema):
        "Teams in the organization."
        stream = "teams"
        loop = asyncio.get_event_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        teams = await loop.run_in_executor(None, self.client.teams, self.state)
        if teams:
            for team in teams:
                singer.write_record(stream, team)
        #extraction_time = singer.utils.now()
        #self.state = singer.write_bookmark(self.state, 'teams', 'dateCreated', singer.utils.strftime(extraction_time))