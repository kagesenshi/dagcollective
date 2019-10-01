import sqlsoup
from sqlalchemy import MetaData, Table
from sqlalchemy import select, func
from sqlalchemy import create_engine
import sqlalchemy.sql.sqltypes as sqltypes
import sqlalchemy.dialects.mssql.base as mssqltypes
import json
import code
import datetime
import progressbar
import fastavro
import os


def json_default(obj):
    if isinstance(obj, datetime.datetime):
        return int(obj.timestamp()*1000)
    elif isinstance(obj, datetime.date):
        return (obj - datetime.date(1970,1,1)).days
    raise TypeError(obj)


def sa_column_to_avro_field(col):
    field = {'name': col.name}
    if col.type.python_type == str:
        field['type'] = ['string', 'null']
    elif isinstance(col.type, sqltypes.INTEGER):
        field['type'] = ['int', 'null']
    elif isinstance(col.type, mssqltypes.TINYINT):
        field['type'] = ['int', 'null']
    elif col.type.python_type == float:
        field['type'] = ['double', 'null']
    elif col.type.python_type == bool:
        field['type'] = ['boolean', 'null']
    elif col.type.python_type == datetime.datetime:
        field['type'] = ['null', {
            'type': 'long',
            'logicalType': 'timestamp-millis',
        }]
    elif col.type.python_type == datetime.date:
        field['type'] = ['null', {
            'type': 'int',
            'logicalType': 'date',
        }]
    else:
        raise TypeError('Unknown Avro type for %s' % type(col.type))
    return field

def get_records(query, columns, t, total):
    print('Dumping %s with %s records' % (t, total))
    bar = progressbar.ProgressBar(maxval=total, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()

    r = query.fetchone()
    counter = 0

    while r is not None:
        data = {}
        for c in columns:
            data[c.name] = getattr(r, c.name)
        yield json.loads(json.dumps(data, default=json_default))
        counter += 1
        if counter % 1000 == 0 and counter <= total:
            bar.update(counter)

        r = query.fetchone()

    bar.update(total)
    bar.finish()


def ingest(dburi, tables, destination, schema=None, namespace=None, limit=None):
    # param:
    #    dburi -> sqlalchemy database connection uri
    #    tables -> list of table names
    #    destination -> path to store the exported data
    #    namespace -> avro namespace for the generated schema

    db = create_engine(dburi)
    metadata = MetaData()

    if namespace is None:
        namespace = db.url.database

    for tblname in tables.items():
        tbl = Table(tblname, metadata, autoload=True, schema=schema, autoload_with=db)
        columns = list(tbl.c)
    
        schema = {'type': 'record', 'namespace': namespace, 'fields': [],
                'name': tblname} 

        for c in columns:
            schema['fields'].append(sa_column_to_avro_field(c))
    
        avsc = fastavro.parse_schema(schema)
    
        counter = 0
        if limit is None:
            s = select([func.count('*').label('total')], from_obj=[tbl])
            total = db.engine.execute(s).fetchall()[0][0]
        else:
            total = limit
    
        selectall = select([getattr(tbl.c,col.name) for col in columns],
                from_obj=[tbl])
    
    
        if limit:
            selectall = selectall.limit(limit)
    
        query = db.execute(selectall)
    
        with open(os.path.join(destination, '%s.avro') % t, 'wb') as f:
            fastavro.writer(f, avsc, get_records(query, columns, t, total),
                    codec='snappy')
    
