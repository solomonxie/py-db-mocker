
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

PG_FUNCTIONS = [
    'nextval',
]

PG_DTYPE_TO_PANDAS = {
    'INT': 'int',
    'VARCHAR': 'str',
    'TIMESTAMPTZ': 'str',  # FIXME
}
