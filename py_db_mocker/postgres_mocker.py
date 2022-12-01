
import re


class PostgresSequence:
    """
    REF: https://www.postgresql.org/docs/current/sql-createsequence.html
    """
    KEYWORDS = [
        'START WITH',
        'INCREMENT BY',
        'NO',
        'MINVALUE',
        'MAXVALUE',
    ]

    def __init__(self, sql):
        upper_sql = str(sql).upper()
        parts = [s.strip() for s in upper_sql.split()]
        if not parts[0:1] != ['CREATE', 'SEQUENCE']:
            raise NotImplementedError(f'TBD: {sql}')
        self.name = None
        self.value = 0
        self.start_with = 0
        self.increment_by = 1
        self.max_value = None
        self.min_value = None
        self.cache = None
        for i, s in enumerate(parts):
            last_token = parts[i - 1]
            last_last_token = parts[i - 2]
            next_token = parts[i + 1] if i + 1 < len(parts) else None
            if last_token == 'SEQUENCE':
                idx = upper_sql.index(s)
                self.name = sql[idx: idx+len(s)]
            elif last_last_token == 'START' and last_token == 'WITH':
                self.value = self.start_with = int(s)
            elif last_last_token == 'INCREMENT' and last_token == 'BY':
                self.increment_by = int(s)
            elif s == 'MINVALUE' and next_token and last_token != 'NO':
                self.min_value = int(next_token)
            elif s == 'MINVALUE' and last_token == 'NO':
                self.min_value = None
            elif s == 'MAXVALUE' and next_token and last_token != 'NO':
                self.max_value = int(next_token)
            elif s == 'MAXVALUE' and last_token == 'NO':
                self.max_value = None
            elif s == 'CACHE' and next_token and last_token != 'NO':
                self.cache = int(next_token)
            elif s == 'CACHE' and last_token == 'NO':
                self.cache = None
            else:
                pass
        return

    def next_val(self):
        self.value += self.increment_by
        if not (self.min_value <= self.value <= self.max_value):
            raise ValueError(f'Sequence value [{self.value}] not in [{self.min_value} - {self.max_value}]')


class FiniteStateMachineParser:
    DAG = {}
    DELIMITER = r''
    KEYWORDS = []

    def __init__(self, sql: str):
        self.handlers = {}
        self.delimiter = re.compile(self.DELIMITER)
        self.next_state(self.DAG, sql.strip())

    def next_state(self, dag: dict, stmt: str):
        start, phrases, matched = 0, [], False
        rest = stmt
        for m in self.delimiter.finditer(stmt):
            half, rest = stmt[start:m.span()[0]].strip(), stmt[m.span()[0]:].strip()
            start = m.span()[1]
            if not half or not rest:
                continue
            if half.upper() in self.KEYWORDS:
                phrases.append(half.upper())
            handler = self.handlers.get(dag.get('STATE'))
            token = ' '.join(phrases) or None
            if any([
                token == dag.get('TOKEN'),
                token in dag.get('OPTIONS', []),
                handler and dag.get('NEXT'),
            ]):
                matched = True
                _ = handler(token, half, rest) if handler else None
                break
        if matched:
            for next_dag in dag.get('NEXT') or []:
                self.next_state(next_dag, rest)
        return


class PostgresAlterTable(FiniteStateMachineParser):
    """
    REF: https://www.postgresql.org/docs/current/sql-altertable.html
    """
    DELIMITER = r'[\'"()\s;]'
    KEYWORDS = [
        'ALTER',
        'TABLE',
        'IF',
        'EXISTS',
        'ONLY',
        'ADD',
        'CONSTRAINT',
        'SET',
        'COLUMN',
        'DEFAULT',
        'PRIMARY',
        'KEY',
        'UNIQUE',
    ]
    DAG = {
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
                                    'NEXT': [{
                                        'STATE': 'set_column_default',
                                    }],
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

    def __init__(self, sql: str):
        self.tablename = None
        self.column_name = None
        self.constraint_name = None
        self.alter_type = None
        self.default_value_map = {}
        self.constraint_map = {}
        self.handlers.update({
            'alter_table_options': self.alter_table_options,
            'set_tablename': self.set_tablename,
            'set_column_name': self.set_column_name,
            'set_column_default': self.set_column_default,
            'set_constraint_name': self.set_constraint_name,
            'set_primary_key': self.set_primary_key,
        })
        super().__init__(sql)

    def alter_table_options(self, token: str, stmt: str, rest: str):
        if token.upper() == 'IF EXISTS':
            pass
        elif token.upper() == 'ONLY':
            pass

    def set_tablename(self, token: str, stmt: str, rest: str):
        self.tablename = str(stmt)

    def set_column_name(self, token: str, stmt: str, rest: str):
        self.column_name = str(stmt)

    def set_column_default(self, token: str, stmt: str, rest: str):
        func_ptn = re.compile(r'\s*\(\'(\s*\w+\s*)\'(::\w+)?\)\s*')
        func_match = func_ptn.match(rest)
        entry = f'{self.tablename}.{self.column_name}'
        if stmt == 'nextval' and func_match:
            sequence_name = func_match.groups()[0].strip()
            self.default_value_map[entry] = {'type': 'function', 'name': stmt, 'value': sequence_name}
        else:
            self.default_value_map[entry] = {'type': type(rest), 'name': 'fixed_value', 'value': rest}
        self.alter_type = 'set_column_default'

    def set_constraint_name(self, token: str, stmt: str, rest: str):
        self.constraint_name = stmt

    def set_primary_key(self, token: str, stmt: str, rest: str):
        ptn = re.compile(r'\s*\(\s*(\w+)\s*\)\s*')
        match = ptn.match(rest)
        if match:
            column_name = match.groups()[0].strip()
            entry = f'{self.tablename}.{column_name}'
            self.constraint_map[entry] = {
                'type': 'primary_key',
                'value': column_name,
            }


def main():
    sql = """
        crEAte  seqUENce  email_email_id_seq
        START WITH 10
        INCREMENT BY 1
        NO MINVALUE
        MAXVALUE 1000
        CACHE 1;
    """
    seq = PostgresSequence(sql)
    __import__('pudb').set_trace()
    assert seq.name == 'email_email_id_seq'
    assert seq.start_with == 10
    assert seq.increment_by == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1
    sql = """
        ALTER  TAbLE OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);
    """
    cs = PostgresAlterTable(sql)
    assert cs.constraint_map.get('email_email.id') is not None
    sql = """
        ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);
    """
    dv = PostgresAlterTable(sql)
    assert dv.default_value_map.get('email_email.id') is not None


if __name__ == '__main__':
    main()
