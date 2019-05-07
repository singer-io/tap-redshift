import copy
import time
from itertools import groupby

import pendulum
import datetime
import pytz
import sys
import simplejson as json

import psycopg2
import singer
import singer.metrics as metrics
from singer import metadata
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_redshift import resolve

__version__ = '1.0.0b9'

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'dbname',
    'user',
    'password',
    'start_date'
]

STRING_TYPES = {'char', 'character', 'nchar', 'bpchar', 'text', 'varchar',
                'character varying', 'nvarchar'}

INTEGER_TYPES = {'bigint', 'int2', 'int', 'int4', 'int8'}

FLOAT_TYPES = {'float', 'float4', 'float8'}

DATE_TYPES = {'date'}

DATETIME_TYPES = {'timestamp', 'timestamptz',
                  'timestamp without time zone', 'timestamp with time zone'}

CONFIG = {}


def discover_catalog(conn, db_schema):
    '''Returns a Catalog describing the structure of the database.'''

    # Use the pg_table_def table for discovery
    # See: https://docs.aws.amazon.com/redshift/latest/dg/r_PG_TABLE_DEF.html
    execute(conn, """set search_path to %s;""", (db_schema,))
    pg_table_def_columns = ["schemaname", "tablename", "column", "type", "encoding", "distkey", "sortkey", "notnull"]
    column_info_raw = execute(conn, """ select "{}"
                                        from pg_table_def
                                        where schemaname != 'pg_catalog'
                                        order by schemaname, tablename """.format('", "'.join(pg_table_def_columns)))

    # Structure: {schema: {table: [{schemaname: thing, tablename: chicken, ...}, ...]
    schema_info = {schema: {table: [dict(zip(pg_table_def_columns, c)) for c in columns]
                            for table, columns in groupby(tables, lambda t: t[1])}
                   for schema, tables
                   in groupby(column_info_raw, lambda t: t[0])}
    
    entries = []
    # TODO: How to get PKs using this method? Does it even make sense
    #       to have PKs for redshift since it's not enforced??
    table_pks = {}
    # TODO: Table Types, can't differentiate between views and tables with this method
    #       - Are views actually retrieved?
    table_types = {}
    for schema_name, tables in schema_info.items():
        for table_name, columns in tables.items():
            qualified_table_name = '{}.{}'.format(schema_name, table_name)
            schema = Schema(type='object',
                            properties={
                                c['column']: schema_for_column(c) for c in columns})
            key_properties = [
                column for column in table_pks.get(table_name, [])
                if schema.properties[column].inclusion != 'unsupported']
            is_view = table_types.get(table_name) == 'VIEW'
            db_name = conn.get_dsn_parameters()['dbname']
            metadata = create_metadata(
                db_name, db_schema, columns, is_view, key_properties)
            tap_stream_id = '{}.{}'.format(
                db_name, qualified_table_name)
            entry = CatalogEntry(
                tap_stream_id=tap_stream_id,
                stream=table_name,
                schema=schema,
                table=qualified_table_name,
                metadata=metadata)

            entries.append(entry)

    return Catalog(entries)


def do_discover(conn, db_schema):
    LOGGER.info("Running discover")
    discover_catalog(conn, db_schema).dump()
    LOGGER.info("Completed discover")


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    column_type = c['type'].lower()
    column_nullable = not c['notnull']
    inclusion = 'available'
    result = Schema(inclusion=inclusion)

    if column_type == 'bool':
        result.type = 'boolean'

    elif column_type in INTEGER_TYPES:
        result.type = 'integer'

    elif column_type in FLOAT_TYPES:
        result.type = 'number'

    elif column_type == 'numeric':
        result.type = 'number'

    elif column_type in STRING_TYPES:
        result.type = 'string'

    elif column_type in DATETIME_TYPES:
        result.type = 'string'
        result.format = 'date-time'

    elif column_type in DATE_TYPES:
        result.type = 'string'
        result.format = 'date'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported column type {}'
                        .format(column_type))

    if column_nullable == 'yes':
        result.type = ['null', result.type]

    return result


def create_metadata(
        db_name, db_schema, cols, is_view, key_properties=[]):
    mdata = metadata.new()
    # TODO: This should follow other DB patterns
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    if not is_view:
        mdata = metadata.write(
            mdata, (), 'table-key-properties', key_properties)
    else:
        mdata = metadata.write(
            mdata, (), 'view-key-properties', key_properties)
    mdata = metadata.write(mdata, (), 'is-view', is_view)
    mdata = metadata.write(mdata, (), 'schema-name', db_schema)
    mdata = metadata.write(mdata, (), 'database-name', db_name)
    valid_rep_keys = []

    for c in cols:
        if c['type'] in DATETIME_TYPES:
            valid_rep_keys.append(c['column'])

        schema = schema_for_column(c)

        mdata = metadata.write(mdata,
                               ('properties', c['column']),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c['column']),
                               'sql-datatype',
                               c['type'].lower())
        # TODO: Inclusion automatic should be used for primary and rep keys
        mdata = metadata.write(mdata,
                               ('properties', c['column']),
                               'inclusion',
                               schema.inclusion)
    if valid_rep_keys:
        mdata = metadata.write(mdata, (), 'valid-replication-keys',
                               valid_rep_keys)
    else:
        mdata = metadata.write(mdata, (), 'forced-replication-method', {
            'replication-method': 'FULL_TABLE',
            'reason': 'No replication keys found from table'})

    return metadata.to_list(mdata)


def open_connection(config):
    host = config['host'],
    port = config['port'],
    dbname = config['dbname'],
    user = config['user'],
    password = config['password']

    connection = psycopg2.connect(
        host=host[0],
        port=port[0],
        dbname=dbname[0],
        user=user[0],
        password=password)
    LOGGER.info('Connected to Redshift')
    return connection


def execute(conn, query, params=tuple()):
    cur = conn.cursor()
    cur.execute(query, params)
    column_specs = cur.fetchall() if cur.rowcount >= 0 else None
    cur.close()
    return column_specs

def get_stream_version(tap_stream_id, state):
    return singer.get_bookmark(state,
                               tap_stream_id,
                               "version") or int(time.time() * 1000)


def row_to_record(catalog_entry, version, row, columns, time_extracted):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        if isinstance(elem, datetime.date):
            elem = elem.isoformat('T') + 'Z'
        row_to_persist += (elem,)
    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=dict(zip(columns, row_to_persist)),
        version=version,
        time_extracted=time_extracted)


def sync_table(connection, catalog_entry, state):
    columns = list(catalog_entry.schema.properties.keys())
    start_date = CONFIG.get('start_date')
    formatted_start_date = None

    if not columns:
        LOGGER.warning(
            'There are no columns selected for table {}, skipping it'
            .format(catalog_entry.table))
        return

    tap_stream_id = catalog_entry.tap_stream_id
    LOGGER.info('Beginning sync for {} table'.format(tap_stream_id))
    with connection.cursor() as cursor:
        schema, table = catalog_entry.table.split('.')
        select = 'SELECT {} FROM {}.{}'.format(
            ','.join('"{}"'.format(c) for c in columns),
            '"{}"'.format(schema),
            '"{}"'.format(table))
        params = {}

        if start_date is not None:
            formatted_start_date = datetime.datetime.strptime(
                start_date, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)

        replication_key = metadata.to_map(catalog_entry.metadata).get(
            (), {}).get('replication-key')
        replication_key_value = None
        bookmark_is_empty = state.get('bookmarks', {}).get(
            tap_stream_id) is None
        stream_version = get_stream_version(tap_stream_id, state)
        state = singer.write_bookmark(
            state,
            tap_stream_id,
            'version',
            stream_version
        )
        activate_version_message = singer.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )

        # If there's a replication key, we want to emit an ACTIVATE_VERSION
        # message at the beginning so the records show up right away. If
        # there's no bookmark at all for this stream, assume it's the very
        # first replication. That is, clients have never seen rows for this
        # stream before, so they can immediately acknowledge the present
        # version.
        if replication_key or bookmark_is_empty:
            yield activate_version_message

        if replication_key:
            replication_key_value = singer.get_bookmark(
                state,
                tap_stream_id,
                'replication_key_value'
            ) or formatted_start_date.isoformat()

        if replication_key_value is not None:
            entry_schema = catalog_entry.schema

            if entry_schema.properties[replication_key].format == 'date-time':
                replication_key_value = pendulum.parse(replication_key_value)

            select += ' WHERE {} >= %(replication_key_value)s ORDER BY {} ' \
                      'ASC'.format(replication_key, replication_key)
            params['replication_key_value'] = replication_key_value

        elif replication_key is not None:
            select += ' ORDER BY {} ASC'.format(replication_key)

        time_extracted = utils.now()
        query_string = cursor.mogrify(select, params)
        LOGGER.info('Running {}'.format(query_string))
        cursor.execute(select, params)
        row = cursor.fetchone()
        rows_saved = 0

        with metrics.record_counter(None) as counter:
            counter.tags['database'] = catalog_entry.database
            counter.tags['table'] = catalog_entry.table
            while row:
                counter.increment()
                rows_saved += 1
                record_message = row_to_record(catalog_entry,
                                               stream_version,
                                               row,
                                               columns,
                                               time_extracted)
                yield record_message

                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  tap_stream_id,
                                                  'replication_key_value',
                                                  record_message.record[
                                                      replication_key])
                if rows_saved % 1000 == 0:
                    yield singer.StateMessage(value=copy.deepcopy(state))
                row = cursor.fetchone()

        if not replication_key:
            yield activate_version_message
            state = singer.write_bookmark(state, catalog_entry.tap_stream_id,
                                          'version', None)

        yield singer.StateMessage(value=copy.deepcopy(state))


def generate_messages(conn, db_schema, catalog, state):
    catalog = resolve.resolve_catalog(discover_catalog(conn, db_schema),
                                      catalog, state)

    for catalog_entry in catalog.streams:
        state = singer.set_currently_syncing(state,
                                             catalog_entry.tap_stream_id)
        catalog_md = metadata.to_map(catalog_entry.metadata)

        if catalog_md.get((), {}).get('is-view'):
            key_properties = catalog_md.get((), {}).get('view-key-properties')
        else:
            key_properties = catalog_md.get((), {}).get('table-key-properties')
        bookmark_properties = catalog_md.get((), {}).get('replication-key')

        # Emit a state message to indicate that we've started this stream
        yield singer.StateMessage(value=copy.deepcopy(state))

        # Emit a SCHEMA message before we sync any records
        yield singer.SchemaMessage(
            stream=catalog_entry.stream,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=bookmark_properties)

        # Emit a RECORD message for each record in the result set
        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = catalog_entry.database
            timer.tags['table'] = catalog_entry.table
            for message in sync_table(conn, catalog_entry, state):
                yield message

    # If we get here, we've finished processing all the streams, so clear
    # currently_syncing from the state and emit a state message.
    state = singer.set_currently_syncing(state, None)
    yield singer.StateMessage(value=copy.deepcopy(state))


def coerce_datetime(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError("Type {} is not serializable".format(type(o)))


def do_sync(conn, db_schema, catalog, state):
    LOGGER.info("Starting Redshift sync")
    for message in generate_messages(conn, db_schema, catalog, state):
        sys.stdout.write(json.dumps(message.asdict(),
                         default=coerce_datetime,
                         use_decimal=True) + '\n')
        sys.stdout.flush()
    LOGGER.info("Completed sync")


def build_state(raw_state, catalog):
    LOGGER.info('Building State from raw state {}'.format(raw_state))

    state = {}

    currently_syncing = singer.get_currently_syncing(raw_state)
    if currently_syncing:
        state = singer.set_currently_syncing(state, currently_syncing)

    for catalog_entry in catalog.streams:
        tap_stream_id = catalog_entry.tap_stream_id
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_method = catalog_metadata.get(
            (), {}).get('replication-method')
        raw_stream_version = singer.get_bookmark(
            raw_state, tap_stream_id, 'version')

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get(
                (), {}).get('replication-key')

            state = singer.write_bookmark(
                state, tap_stream_id, 'replication_key', replication_key)

            # Only keep the existing replication_key_value if the
            # replication_key hasn't changed.
            raw_replication_key = singer.get_bookmark(raw_state,
                                                      tap_stream_id,
                                                      'replication_key')
            if raw_replication_key == replication_key:
                raw_replication_key_value = singer.get_bookmark(
                    raw_state, tap_stream_id, 'replication_key_value')
                state = singer.write_bookmark(state,
                                              tap_stream_id,
                                              'replication_key_value',
                                              raw_replication_key_value)

            if raw_stream_version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', raw_stream_version)

        elif replication_method == 'FULL_TABLE' and raw_stream_version is None:
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          'version',
                                          raw_stream_version)

    return state


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    connection = open_connection(args.config)
    db_schema = args.config.get('schema', 'public')
    if args.discover:
        do_discover(connection, db_schema)
    elif args.catalog:
        state = build_state(args.state, args.catalog)
        do_sync(connection, db_schema, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = build_state(args.state, catalog)
        do_sync(connection, db_schema, catalog, state)
    else:
        LOGGER.info("No properties were selected")


if __name__ == '__main__':
    main()
