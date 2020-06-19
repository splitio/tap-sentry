#!/usr/bin/env python
from setuptools import setup

setup(
      name="tap-sentry",
      version="0.1.0",
      description="Singer.io tap for extracting Sentry project events and project issues.",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_sentry"],
      install_requires=[
            "singer-python>=5.0.12",
            "requests",
            "pendulum"
      ],
      entry_points="""
      [console_scripts]
      tap-sentry=tap_sentry:main
      """,
      packages=["tap_sentry"],
      package_data = {
            "schemas": ["tap_sentry/schemas/*.json"]
      },
      include_package_data=True,
)
