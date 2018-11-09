#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
from tempfile import NamedTemporaryFile

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from target_postgres.db_sync import DbSync

logger = singer.get_logger()


STATE = {}


def emit_bookmark(stream, bookmark):
    STATE['bookmarks'] = {stream: bookmark}
    singer.write_state(STATE)


def persist_lines(config, lines):
    bookmarks = {}
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    csv_files_to_load = {}
    row_count = {}
    stream_to_sync = {}
    primary_key_exists = {}
    batch_size = int(config.get('batch_size', 100_000))

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            # Validate record
            validators[stream].validate(o['record'])

            sync = stream_to_sync[stream]

            primary_key_string = sync.record_primary_key_string(o['record'])
            if stream not in primary_key_exists:
                primary_key_exists[stream] = {}
            if primary_key_string and primary_key_string in primary_key_exists[stream]:
                flush_records(stream, csv_files_to_load, row_count, primary_key_exists, bookmarks, sync)

            csv_line = sync.record_to_csv_line(o['record'])
            csv_files_to_load[stream].write(bytes(csv_line + '\n', 'UTF-8'))
            row_count[stream] += 1
            if primary_key_string:
                primary_key_exists[stream][primary_key_string] = True

            if row_count[stream] >= batch_size:
                flush_records(stream, csv_files_to_load, row_count, primary_key_exists, bookmarks, sync)
        elif t == 'STATE':
            # update each bookmarks
            state = o['value']
            logger.info(f"STATE: {state}")
            for stream, bookmark in state.get('bookmarks', {}).items():
                logger.info(f"{stream}: {bookmark}")
                bookmarks[stream] = bookmark
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
            stream_to_sync[stream] = DbSync(config, o)
            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()
            row_count[stream] = 0
            csv_files_to_load[stream] = NamedTemporaryFile(mode='w+b')
        elif t == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    streams = (stream
               for stream, count in row_count.items()
               if count)

    for stream in streams:
        flush_records(stream,
                      csv_files_to_load,
                      row_count,
                      primary_key_exists,
                      bookmarks,
                      stream_to_sync[stream])


def flush_records(stream, csv_files_to_load, row_count, primary_key_exists, bookmarks, sync):
    sync.load_csv(csv_files_to_load[stream], row_count[stream])
    row_count[stream] = 0
    primary_key_exists[stream] = {}
    csv_files_to_load[stream] = NamedTemporaryFile(mode='w+b')
    emit_bookmark(stream, bookmarks[stream])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, input)

    singer.write_state(STATE)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
