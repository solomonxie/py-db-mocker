
import re
import yaml

from py_db_mocker.constants import PG_KEYWORDS
from py_db_mocker.constants import SEGMENT_TYPE
from py_db_mocker.constants import SQL_SEGMENT
from py_db_mocker.constants import PG_FUNCTIONS


class FiniteStateMachineParser:
    DELIMITER = re.compile(r'[\'"()\s;]')
    KEYWORDS = []
    STATE_MACHINE_PATH = ''

    def __init__(self, sql: str):
        self.definition = yaml.safe_load(open(self.STATE_MACHINE_PATH).read())
        # Stateful variables
        self.token = None
        self.segment = None
        self.seg_type = None
        self.tail = sql
        # Start parsing at initiation
        self.run_dag(self.definition)

    @property
    def handlers(self):
        return {}

    @property
    def functions(self):
        return {}

    def run_dag(self, dag: dict):
        statename = dag.get('STATE')
        handler = self.handlers.get(statename)
        _ = handler() if handler else None
        option_map = {d.get('TOKEN'): d for d in dag.get('OPTIONS', [])}
        next_map = {d.get('TOKEN'): d for d in dag.get('NEXT', [])}
        self._set_next_segment()
        next_dag = next_map.get(self.token) or (dag['NEXT'][0] if len(next_map) == 1 else None)
        while option_map.get(self.token):
            self.run_dag(option_map[self.token])
        if next_dag:
            self.run_dag(next_dag)
        elif dag.get('NEXT') and not next_dag:
            raise ValueError('No state found')
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
                self.seg_type = seg_type
                self.token = token if token in PG_KEYWORDS else None
                self.tail = self.tail[m.span()[1]:]
                return
        return


class PostgresAlterTable(FiniteStateMachineParser):
    """
    REF: https://www.postgresql.org/docs/current/sql-altertable.html
    """
    STATE_MACHINE_PATH = './py_db_mocker/dag_pg_alter_table.yaml'

    def __init__(self, sql: str, sequence_map: dict = None):
        self.table_if_exists = False
        self.table_only = False
        self.tablename = None
        self.column_name = None
        self.function_name = None
        self.constraint_name = None
        self.alter_type = None
        self.default_value_map = {}
        self.constraint_map = {}
        self.sequence_map = sequence_map or {}
        super().__init__(sql)

    @property
    def handlers(self):
        return {
            'set_table_if_exists': self.set_table_if_exists,
            'set_table_only': self.set_table_only,
            'set_tablename': self.set_tablename,
            'set_column_name': self.set_column_name,
            'set_column_default': self.set_column_default,
            'set_value_from_func': self.set_value_from_func,
            'set_action_token': self.set_action_token,
            'set_constraint_name': self.set_constraint_name,
            'set_primary_key': self.set_primary_key,
        }

    @property
    def functions(self):
        return {
            'nextval': self.func_nextval,
        }

    def set_table_if_exists(self):
        self.table_if_exists = True

    def set_table_only(self):
        self.table_only = True

    def set_tablename(self):
        self.tablename = str(self.segment)

    def set_column_name(self):
        self.column_name = str(self.segment)

    def set_column_default(self):
        entry = f'{self.tablename}.{self.column_name}'
        if self.segment in PG_FUNCTIONS:
            self.func = self.functions.get(self.segment)
        else:
            self.default_value_map[entry] = self.segment

    def set_value_from_func(self):
        entry = f'{self.tablename}.{self.column_name}'
        self.default_value_map[entry] = self.segment
        ptn = re.compile(r'\(([^()]+)\)')
        match = ptn.match(self.segment)
        if match:
            params = match.groups()[0].split(',')
            self.func(*params)

    def set_action_token(self):
        if self.token == 'COLUMN':
            self._set_next_segment()

    def set_constraint_name(self):
        self.constraint_name = str(self.segment)

    def set_primary_key(self):
        ptn = re.compile(r'\(([^()]+)\)')
        match = ptn.match(self.segment)
        if match:
            names = match.groups()[0].split(',')
            for col in names:
                entry = f'{self.tablename}.{col}'
                self.constraint_map[entry] = {
                    'type': 'primary_key',
                    'value': col,
                }

    def func_nextval(self, seq_class, *args, **kwargs):
        ptn = re.compile(r"'(?P<SEQ_NAME>\w+)'(::(?P<CLASS_NAME>\w+))?")
        match = ptn.match(seq_class)
        if match:
            d = match.groupdict()
            seqname = d.get('SEQ_NAME')
            seq = self.sequence_map.get(seqname)
            if seq:
                return seq.next_value()


class PostgresCreateSequence(FiniteStateMachineParser):
    """
    REF: https://www.postgresql.org/docs/15/sql-createsequence.html
    """
    STATE_MACHINE_PATH = './py_db_mocker/dag_pg_create_sequence.yaml'

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
        ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);
    """
    dv = PostgresAlterTable(sql)
    __import__('pudb').set_trace()
    assert dv.default_value_map.get('email_email.id') is not None

    sql = """
        ALTER  TAbLE IF EXiSTS OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);
    """
    cs = PostgresAlterTable(sql)
    __import__('pudb').set_trace()
    assert cs.constraint_map['email_email.id'] == {'type': 'primary_key', 'value': 'id'}

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
    __import__('pudb').set_trace()
    assert seq.name == 'email_email_id_seq'
    assert seq.start_with == 10
    assert seq.increment_by == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1


if __name__ == '__main__':
    main()
