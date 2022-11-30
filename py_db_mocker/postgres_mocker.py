

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
    DELIMITERS = []
    TOKENS = []


class PostgresAlterTableParser(FiniteStateMachineParser):
    DELIMITER = r'[\'"()\s;]'
    TOKENS = []
    DAG = {
        'TOKEN': 'ALTER TABLE',
        'STATE': 'alter_table',
        'NEXT': [{
            'OPTIONS': ['IF EXISTS', 'ONLY'],
            'NEXT': [{
                'STATE': 'tablename',
                'NEXT': [
                    {
                        'TOKEN': 'ALTER COLUMN',
                        'STATE': 'alter_column',
                        'NEXT': [
                            {
                                'TOKEN': 'SET DEFAULT',
                                'STATE': 'set_column_default',
                            }
                        ],
                    },
                    {
                        'TOKEN': 'ADD CONSTRAINT',
                        'STATE': 'add_table_constraint',
                    },
                ]
            }],
        }],
    }

    def __init__(self, sql: str):
        import re
        self.check_exists = False
        self.only = None
        self.tablename = None
        self.delimiter = re.compile(self.DELIMITER)
        self.next_state(self.DAG, sql.strip())
        # ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);

    def next_state(self, dag: dict, stmt: str):
        __import__('pudb').set_trace()
        for x in self.delimiter.finditer(stmt):
            pass
        return


class PostgresAlterTableCondition:
    """
    REF: https://www.postgresql.org/docs/current/sql-altertable.html
    """
    KEYWORDS = [
        'IF',
        'EXISTS',
        'ONLY',
    ]
    ACTIONS = [
        'ADD',
        'DROP',
        'ALTER',
        'SET',
    ]

    def __init__(self, sql: str):
        upper_sql = str(sql).upper()
        parts = [s.strip() for s in upper_sql.split()]
        if not parts[0:1] != ['ALTER', 'TABLE']:
            raise NotImplementedError(f'TBD: {sql}')
        parser = PostgresAlterTableParser(sql)
        __import__('pudb').set_trace()
        self.tablename = None
        self.default_value = None
        self.constraint = None
        for i, s in enumerate(parts):
            __import__('pudb').set_trace()
            last_token = parts[i - 1] if i > 0 else None
            last_last_token = parts[i - 2] if i > 1 else None
            next_token = parts[i + 1] if i + 1 < len(parts) else None
            if not self.tablename and s in self.ACTIONS and last_token:
                idx = upper_sql.index(last_token)
                self.tablename = sql[idx: idx+len(last_token)]
            # if last_last_token == 'START' and last_token == 'WITH':
            #     self.value = self.start_with = int(s)
            # else:
            #     pass
        return


class PostgresColumnDefaultValue:
    def __init__(self, action: str):
        return


class PostgresTableConstraint:
    def __init__(self, action: str):
        return


def main():
    sql = """
        ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);
    """
    dv = PostgresAlterTableCondition(sql)
    __import__('pudb').set_trace()

    sql = """
        ALTER  TAbLE OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);
    """
    cs = PostgresAlterTableCondition(sql)
    __import__('pudb').set_trace()

    sql = """
        crEAte  seqUENce  email_email_id_seq
        START WITH 10
        INCREMENT BY 1
        NO MINVALUE
        MAXVALUE 1000
        CACHE 1;
    """
    seq = PostgresSequence(sql)
    assert seq.name == 'email_email_id_seq'
    assert seq.start_with == 10
    assert seq.increment_by == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1


if __name__ == '__main__':
    main()
