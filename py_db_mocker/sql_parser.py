import re
import yaml

from py_db_mocker.constants import PG_KEYWORDS
from py_db_mocker.constants import SEGMENT_TYPE
from py_db_mocker.constants import SQL_SEGMENT


class FiniteStateMachineParser:
    DELIMITER = re.compile(r'[\'"()\s;]')
    KEYWORDS = []
    STATE_MACHINE_PATH = ''

    def __init__(self, sql: str, **kwargs):
        self.definition = yaml.safe_load(open(self.STATE_MACHINE_PATH).read())
        # Stateful variables
        self.token = None
        self.segment = None
        self.seg_type = None
        self.tail = sql
        if sql:
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
