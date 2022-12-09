
import re
import yaml

from py_db_mocker.constants import PG_KEYWORDS
from py_db_mocker.constants import SEGMENT_TYPE
from py_db_mocker.constants import SQL_SEGMENT
from py_db_mocker.constants import PG_ALTER_TABLE_DAG


class FiniteStateMachineParser:
    DELIMITER = re.compile(r'[\'"()\s;]')
    KEYWORDS = []
    DAG_PATH = ''

    def __init__(self, sql: str):
        # self.next_state(self.DAG, sql.strip())
        __import__('pudb').set_trace()
        self.dag = yaml.safe_load(open(self.DAG_PATH).read())
        self.parse_sql(self.dag, sql.strip())

    @property
    def handlers(self):
        return {}

    def parse_sql(self, states: list, sql: str):
        pass


class PostgresAlterTable(FiniteStateMachineParser):
    """
    REF: https://www.postgresql.org/docs/current/sql-altertable.html
    """
    DAG = PG_ALTER_TABLE_DAG

    def __init__(self, sql: str):
        self.tablename = None
        self.column_name = None
        self.constraint_name = None
        self.alter_type = None
        self.default_value_map = {}
        self.constraint_map = {}
        super().__init__(sql)

    @property
    def handlers(self):
        return {
            'alter_table_options': self.alter_table_options,
            'set_tablename': self.set_tablename,
            'set_column_name': self.set_column_name,
            'set_column_default': self.set_column_default,
            'set_constraint_name': self.set_constraint_name,
            'set_primary_key': self.set_primary_key,
        }

    def next_state(self, dag: dict, stmt: str):
        start, phrases, matched = 0, [], False
        tail = stmt
        for m in self.DELIMITER.finditer(stmt):
            head, tail = stmt[start:m.span()[0]].strip(), stmt[m.span()[0]:].strip()
            start = m.span()[1]
            if not head or not tail:
                continue
            if head.upper() in PG_KEYWORDS:
                phrases.append(head.upper())
            handler = self.handlers.get(dag.get('STATE'))
            token = ' '.join(phrases) or None
            if any([
                token and token == dag.get('TOKEN'),
                token and token in dag.get('OPTIONS', []),
                handler and dag.get('NEXT'),
            ]):
                matched = True
                break
        _ = handler(token, head, tail) if handler else None
        if matched:
            # FIXME
            for next_dag in dag.get('NEXT') or []:
                self.next_state(next_dag, tail)
        return

    def alter_table_options(self, token: str, stmt: str, tail: str):
        if token.upper() == 'IF EXISTS':
            pass
        elif token.upper() == 'ONLY':
            pass

    def set_tablename(self, token: str, stmt: str, tail: str):
        self.tablename = str(stmt)

    def set_column_name(self, token: str, stmt: str, tail: str):
        self.column_name = str(stmt)

    def set_column_default(self, token: str, stmt: str, tail: str):
        func_ptn = re.compile(r'\s*\(\'(\s*\w+\s*)\'(::\w+)?\)\s*')
        func_match = func_ptn.match(tail)
        entry = f'{self.tablename}.{self.column_name}'
        if stmt == 'nextval' and func_match:
            sequence_name = func_match.groups()[0].strip()
            self.default_value_map[entry] = {'type': 'function', 'name': stmt, 'value': sequence_name}
        else:
            self.default_value_map[entry] = {'type': type(tail), 'name': 'fixed_value', 'value': tail}
        self.alter_type = 'set_column_default'

    def set_constraint_name(self, token: str, stmt: str, tail: str):
        self.constraint_name = stmt

    def set_primary_key(self, token: str, stmt: str, tail: str):
        ptn = re.compile(r'\s*\(\s*(\w+)\s*\)\s*')
        match = ptn.match(tail)
        if match:
            column_name = match.groups()[0].strip()
            entry = f'{self.tablename}.{column_name}'
            self.constraint_map[entry] = {
                'type': 'primary_key',
                'value': column_name,
            }


class PostgresCreateSequence(FiniteStateMachineParser):
    """
    REF: https://www.postgresql.org/docs/15/sql-createsequence.html
    """
    DAG_PATH = './py_db_mocker/dag_pg_create_sequence.yaml'

    def __init__(self, sql):
        self.name = None
        self.start_with = None
        self.increment_by = None
        self.min_value = None
        self.max_value = None
        self.cache = None
        # Stateful variables
        self.state = None
        self.token = None
        self.segment = None
        self.seg_type = None
        self.tail = sql
        super().__init__(sql)

    @property
    def handlers(self):
        return {
            'set_sequence_name': self.set_sequence_name,
        }

    # def parse_sql(self, states: list, sql: str):
    #     __import__('pudb').set_trace()
    #     segment, seg_type, tail = parse_next_sql_segment(sql)
    #     for dag in states:
    #         handler = self.handlers.get(dag.get('STATE'))
    #         token = dag.get('TOKEN')
    #         matches = [
    #             token and token == dag.get('TOKEN'),
    #             token and token in dag.get('OPTIONS', []),
    #             handler and dag.get('NEXT'),
    #         ]
    #         if any(matches):
    #             _ = handler(segment, tail) if handler else None
    #     return states, tail

    def next_states(self, states: list, sql: str):
        pass

    def next_segment(self) -> str:
        try:
            m = next(SQL_SEGMENT.finditer(self.tail))
        except StopIteration:
            return ''
        for seg_type, segment in m.groupdict().items():
            token = segment.upper() if segment else None
            if seg_type == SEGMENT_TYPE.TOKEN and token not in PG_KEYWORDS:
                seg_type = SEGMENT_TYPE.NAME
            if segment:
                self.segment = segment
                self.token = token
                self.seg_type = seg_type
                self.tail = self.tail[m.span()[1]:]
                return self.tail
        return ''

    def set_sequence_name(self, *args, **kwargs):
        pass


def main():
    sql = """
        crEAte  seqUENce  email_email_id_seq
        START WITH 10
        INCREMENT BY 1
        NO MINVALUE
        MAXVALUE 1000
        CACHE 1
        default nextval('email_email_id_seq'::regclass)
        ;
    """
    seq = PostgresCreateSequence(sql)
    # seq = PostgresSequence(sql)
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
