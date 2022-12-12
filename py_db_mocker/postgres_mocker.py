
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
        self.dag = yaml.safe_load(open(self.DAG_PATH).read())
        # Stateful variables
        self.state = None
        self.token = None
        self.segment = None
        self.seg_type = None
        self.tail = sql
        # Start parsing at initiation
        self.next_state(self.dag.get('NEXT', []))

    @property
    def handlers(self):
        return {}

    def next_state(self, states: list):
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
        super().__init__(sql)

    @property
    def handlers(self):
        return {
            'set_sequence_name': self.set_sequence_name,
            'set_start_value': self.set_start_value,
            'set_increment_value': self.set_increment_value,
            'set_minvalue': self.set_minvalue,
            'set_maxvalue': self.set_maxvalue,
            'set_no_minvalue': self.set_no_minvalue,
            'set_no_maxvalue': self.set_no_maxvalue,
            'set_cache_value': self.set_cache_value,
        }

    def next_state(self, states: list):
        self._set_next_segment()
        self.current_dag = self._match_state(states)
        state = self.current_dag.get('STATE') or ''
        handler = self.handlers.get(state)
        _ = handler() if handler else None
        # Go to next state
        if self.current_dag.get('NEXT'):
            self.next_state(self.current_dag['NEXT'])
        elif self.current_dag.get('OPTIONS'):
            self.next_options(self.current_dag['OPTIONS'])
        return

    def next_options(self, states: list):
        # Multiple-options are allowed, each MUST starts with a TOKEN
        self._set_next_segment()
        option_map = {d.get('TOKEN'): d for d in states}
        while self.token in option_map:
            dag = option_map[self.token]
            state = dag.get('STATE')
            handler = self.handlers.get(state)
            _ = handler() if handler else None
            if dag.get('NEXT'):
                self.next_state(dag['NEXT'])
            elif dag.get('OPTIONS'):
                self.next_options(dag['OPTIONS'])
            self._set_next_segment()
        return

    def _match_state(self, states: list) -> dict:
        if len(states) == 1:
            return states[0]
        for dag in states:
            if self.token == dag.get('TOKEN') is not None:
                return dag
        raise ValueError('No state found')

    def _set_next_segment(self) -> None:
        try:
            m = next(SQL_SEGMENT.finditer(self.tail))
        except StopIteration:
            return
        for seg_type, segment in m.groupdict().items():
            token = segment.upper() if segment else None
            if seg_type == SEGMENT_TYPE.TOKEN and token not in PG_KEYWORDS:
                seg_type = SEGMENT_TYPE.NAME
            if segment:
                self.segment = segment
                self.token = token
                self.seg_type = seg_type
                self.tail = self.tail[m.span()[1]:]
                return
        return

    def set_sequence_name(self):
        self.name = self.segment

    def set_start_value(self):
        if self.token == 'WITH':
            self._set_next_segment()
        self.start_with = float(self.segment)

    def set_increment_value(self):
        if self.token == 'BY':
            self._set_next_segment()
        self.increment_by = float(self.segment)

    def set_minvalue(self):
        self.min_value = float(self.segment)

    def set_maxvalue(self):
        self.max_value = float(self.segment)

    def set_no_minvalue(self):
        self.min_value = None

    def set_no_maxvalue(self):
        self.max_value = None

    def set_cache_value(self):
        self.cache = float(self.segment)


def main():
    sql = """
        crEAte  seqUENce  email_email_id_seq
        START 10
        INCREMENT BY 1
        NO MINVALUE
        MAXVALUE 1000
        CACHE 1
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
