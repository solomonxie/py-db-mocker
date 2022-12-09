
import re

SQL_SEGMENT = re.compile(r'(?P<TOKEN>\w+)|(?P<SUBEXP>\(.+\))|(?P<LITERAL>\'[^\']+\')|(?P<NUM>\d+)|(?P<FLOAT>\d+\.\d+)')

class SEGMENT_TYPE:
    LITERAL = 'LITERAL'
    FUNC = 'FUNC'
    SUBEXPRESSION = 'SUBEXP'
    TOKEN = 'TOKEN'
    NUMBER = 'NUM'
    NAME = 'NAME'


PG_KEYWORDS = [
    'ADD',
    'ALTER',
    'AS',
    'BY',
    'CACHE',
    'COLUMN',
    'CONSTRAINT',
    'CREATE',
    'CYCLE',
    'DEFAULT',
    'EXISTS',
    'IF',
    'INCREMENT',
    'KEY',
    'MAXVALUE',
    'MINVALUE',
    'NO',
    'NOT',
    'ONLY',
    'OWNED',
    'PRIMARY',
    'SEQUENCE',
    'SET',
    'START',
    'TABLE',
    'UNIQUE',
    'WITH',
]

PG_ALTER_TABLE_DAG = {
    'TOKEN': 'ALTER TABLE',
    'STATE': 'alter_table',
    'NEXT': [{
        'STATE': 'alter_table_options',
        'OPTIONS': ['IF EXISTS', 'ONLY'],
        'NEXT': [{
            'STATE': 'set_tablename',
            'NEXT': [
                {
                    'TOKEN': 'ALTER COLUMN',
                    'STATE': 'alter_column',
                    'NEXT': [{
                        'STATE': 'set_column_name',
                        'NEXT': [
                            {
                                'TOKEN': 'SET DEFAULT',
                                'NEXT': [{'STATE': 'set_column_default'}],
                            }
                        ],
                    }],
                },
                {
                    'TOKEN': 'ADD CONSTRAINT',
                    'STATE': 'add_table_constraint',
                    'NEXT': [{
                        'STATE': 'set_constraint_name',
                        'NEXT': [
                            {
                                'TOKEN': 'PRIMARY KEY',
                                'STATE': 'set_primary_key',
                            },
                        ],
                    }],
                },
            ]
        }],
    }],
}
