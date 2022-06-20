#!/usr/bin/env python3
import os
import json
from pickle import NEXT_BUFFER
from sqlite3 import threadsafety
import singer
from singer import utils, metadata, metrics
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests
from tap_pleo.context import Context
from datetime import date

BASE_URL = "https://openapi.pleo.io/v1/"
REQUIRED_CONFIG_KEYS = ["start_date", "bearer_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)

def sync_stream(streamname, start_date=None, end_date=None):
    if streamname == 'balance':
        url = f"{BASE_URL}/companies/{streamname}"
    elif streamname == 'tax-codes':
        url = f"{BASE_URL}{streamname}"
    else:
        url = f"{BASE_URL}{streamname}?pageOffset=0&pageSize=100"
    headers = {'Accept': 'application/json', 'Authorization': 'Bearer {token}'.format(token=Context.config.get('bearer_token'))}

    session = requests.Session()    

    response = session.request("GET", url, headers=headers)
    LOGGER.info("API request: " + url)
  #  first_page = response.json()
    first_page = response.text
    response_json = json.loads(first_page)
    yield response_json
    
    if 'metadata' in response_json:
        
        LOGGER.info(f"Metadata: {response_json['metadata']}")
        nextpageoffset = response_json['metadata']['pageInfo']['nextPageOffset']
        totalCount = response_json['metadata']['pageInfo']['totalCount']
        
        LOGGER.info(f"Next Page Offset: {nextpageoffset}")
        #return response_json
        #LOGGER.info(f"Batch 1 of {number_pages}")
        
        while nextpageoffset:
            LOGGER.info('Starting new Page')
            url = f"{BASE_URL}{streamname}?pageOffset={nextpageoffset}&pageSize=100"
            response = session.request("GET", url, headers=headers)
            next_page = response.json()

            nextpageoffset = next_page['metadata']['pageInfo']['nextPageOffset']
            yield next_page

def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    counter = 0
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        for batch in sync_stream(stream.tap_stream_id):
            if type(batch) is list:
                LOGGER.info('Batch ist keine Liste ')
                with singer.metrics.record_counter(endpoint=stream.tap_stream_id) as counter:
                    for row in batch:
                        singer.write_record(stream.tap_stream_id, row)
                        if bookmark_column:
                            if is_sorted:
                                # update bookmark to latest value
                                singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                            else:
                                # if data unsorted, save max value until end of writes
                                max_bookmark = max(max_bookmark, row[bookmark_column])
                        counter.increment()
                    if bookmark_column and not is_sorted:
                        singer.write_state({stream.tap_stream_id: max_bookmark})    
            else:
                if batch.get('metadata') == None:
                    if stream.tap_stream_id == 'balance':
                        LOGGER.warning('Konnte nicht abgespielt werde :(')
                        LOGGER.info(batch)
                        singer.write_record(stream.tap_stream_id, batch)
                    elif stream.tap_stream_id == 'tax-codes':
                        with singer.metrics.record_counter(endpoint=stream.tap_stream_id) as counter:
                            try:
                                for row in batch['taxCodes']:
                                    singer.write_record(stream.tap_stream_id, row)
                                    if bookmark_column:
                                        if is_sorted:
                                            # update bookmark to latest value
                                            singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                                        else:
                                            # if data unsorted, save max value until end of writes
                                            max_bookmark = max(max_bookmark, row[bookmark_column])
                                    counter.increment()
                            except Exception as e: 
                                LOGGER.warning('Konnte nicht abgespielt werde :(')
                                singer.write_record(stream.tap_stream_id, batch)
                            if bookmark_column and not is_sorted:
                                singer.write_state({stream.tap_stream_id: max_bookmark})

                        
                else:
                    if batch['metadata']:
                        Context.new_counts[stream.tap_stream_id] = batch['metadata']['pageInfo']['totalCount']   
                
                    with singer.metrics.record_counter(endpoint=stream.tap_stream_id) as counter:
                        try:
                            for row in batch[stream.tap_stream_id]:
                                singer.write_record(stream.tap_stream_id, row)
                                if bookmark_column:
                                    if is_sorted:
                                        # update bookmark to latest value
                                        singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                                    else:
                                        # if data unsorted, save max value until end of writes
                                        max_bookmark = max(max_bookmark, row[bookmark_column])
                                counter.increment()
                        except Exception as e: 
                            LOGGER.warning('Konnte nicht abgespielt werde :(')
                            singer.write_record(stream.tap_stream_id, batch)
                        if bookmark_column and not is_sorted:
                            singer.write_state({stream.tap_stream_id: max_bookmark})
        try:
            LOGGER.info(f"Extracted {Context.new_counts[stream.tap_stream_id]} records from {stream.tap_stream_id}")
        except:
            pass    
    


        
       

        #LOGGER.info(f"Response: {response_json}")

        # TODO: delete and replace this inline function with your own data retrieval process:
        #tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]

        max_bookmark = None

        #for row in result[stream.tap_stream_id]:
         #   singer.write_record(stream.tap_stream_id, row)

        #for row in tap_data():
        #    # TODO: place type conversions or transformations here#

            # write one or more rows to the stream:
        #    singer.write_records(stream.tap_stream_id, [row])
        #    if bookmark_column:
        #        if is_sorted:
        #            # update bookmark to latest value
        #            singer.write_state({stream.tap_stream_id: row[bookmark_column]})
        #        else:
        #            # if data unsorted, save max value until end of writes
        #            max_bookmark = max(max_bookmark, row[bookmark_column])
        #if bookmark_column and not is_sorted:
        #    singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    LOGGER.debug(f"Argumentss: {args}")

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        Context.tap_start = utils.now()
        LOGGER.info("Tap Start: " + str(Context.tap_start))
        if args.catalog:
            Context.catalog = args.catalog#.to_dict()
            #LOGGER.info(f"Catalog: {catalog}")
        else:
            Context.catalog = discover()
            
        Context.config = args.config
        Context.state = args.state

        sync(Context.config, Context.state, Context.catalog)


if __name__ == "__main__":
    main()
    