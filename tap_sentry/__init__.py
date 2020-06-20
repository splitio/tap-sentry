#!/usr/bin/env python3
import os
import json
import singer
import asyncio
import concurrent.futures
from singer import utils, metadata
from singer.catalog import Catalog

from tap_sentry.sync import SentryAuthentication, SentryClient, SentrySync

REQUIRED_CONFIG_KEYS = ["start_date",
                        "api_token"]
LOGGER = singer.get_logger()

# map of schema name with their primary key
SCHEMA_PRIMARY_KEYS = { 
    "issues": ["id"],
    "projects": ["id"],
    "teams": ["id"],
    "users": ["id"]
    # "events": ["id"], this has a lot of data, commenting it out for now.
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)



def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    refs = schema.pop("definitions", {})
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def generate_metadata(schema_name, schema):
    pk_fields = SCHEMA_PRIMARY_KEYS[schema_name]
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', pk_fields)

    for field_name in schema['properties'].keys():
        if field_name in pk_fields:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)






def discover():
    streams = []

    for schema_name in SCHEMA_PRIMARY_KEYS.keys():

        schema = load_schema(schema_name) ##load the schema for each of them
        stream_metadata = generate_metadata(schema_name, schema)
        stream_key_properties = SCHEMA_PRIMARY_KEYS[schema_name]

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : stream_metadata,
            'key_properties': stream_key_properties
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams

def create_sync_tasks(config, state, catalog):
    auth = SentryAuthentication(config["api_token"])
    client = SentryClient(auth)
    sync = SentrySync(client, state)

    # selected_stream_ids = get_selected_streams(catalog)
    # sync_tasks = (sync.sync(stream.tap_stream_id, stream.schema)
    #               for stream in catalog.streams
                #   if stream.tap_stream_id in selected_stream_ids)

    sync_tasks = (sync.sync(stream['tap_stream_id'], stream['schema'])
        for stream in catalog['streams'])
    

    return asyncio.gather(*sync_tasks)

def  sync(config, state, catalog):
    loop = asyncio.get_event_loop()
    try:
        tasks = create_sync_tasks(config, state, catalog)
        loop.run_until_complete(tasks)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        config = args.config
        state ={
            "bookmarks": {
               "issues": {"start": config["start_date"]},
                "events": {"start": config["start_date"]}
            }
        }
        state.update(args.state)

        sync(config, state, catalog)

if __name__ == "__main__":
    main()
