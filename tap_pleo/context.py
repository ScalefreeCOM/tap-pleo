
import logging
import singer
from singer import utils, Transformer, metrics
from singer import metadata

LOGGER = singer.get_logger()

class Context():
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}
    
    auth_token = None

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def print_counts(cls):
        # Separate loops for formatting.
        for stream_name, stream_count in Context.new_counts.items():
            with metrics.record_counter(stream_name) as counter:
                updates_count = Context.updated_counts[stream_name]
                total_replicated = stream_count + updates_count
                counter.increment(total_replicated)

        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')