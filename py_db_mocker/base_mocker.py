"""
REF: https://media.geeksforgeeks.org/wp-content/uploads/20210920153429/new.png
Data Query Language: SELECT
Data Definition Language: CREATE/DROP/ALTER/TRUNCATE
Data Manipulation Language: INSERT/UPDATE/DELETE/CALL/EXPLAIN CALL/LOCK
Transaction Control Language: COMMIT/ROLLBACK/SAVEPOINT/SAVE TRANSACTION/SET CONSTRAINT
Data Control Langauge: GRANT/INVOKE
"""
import logging
from copy import deepcopy

import pandas as pd
from sqlglot import parse, exp
from sqlglot.dialects import Postgres
from sqlglot.expressions import Expression

from py_db_mocker.constants import PG_DTYPE_TO_PANDAS


logger = logging.getLogger(__name__)


class BaseRelationalDBMocker:
    DIALECT = None
    KEYWORDS = []

    def __init__(self, *args, **kwargs):
        self.table_map = {}
        self.sequence_map = {}
        self.constraint_map = {}
        self.default_value_map = {}
        self.table_backup = None

    def execute(self, sql: str, params: dict = None):
        sql = self.compile_sql(sql, params)
        ast_list = parse(sql, self.DIALECT)
        execution_steps = self.regroup_execution_steps(ast_list)
        rows = []
        for step in execution_steps:
            if isinstance(step, (list,)):
                rows = self.transaction(step)
            else:
                rows = self.execute_one(step)
        return rows

    def execute_one(self, ast: Expression) -> list:
        rows = []
        sql = ast.sql().upper()
        tokens = [k for k in self.KEYWORDS if k in sql]
        if 'SELECT' in tokens:
            rows = self.select(ast)
        elif 'CREATE' in tokens and 'TABLE' in tokens:
            rows = self.create_table(ast)
        elif 'CREATE' in tokens and 'SEQUENCE' in tokens:
            rows = self.create_sequence(ast)
        elif 'ALTER' in tokens and 'TABLE' in tokens:
            rows = self.alter_table(ast)
        elif 'BEGIN' in tokens:
            pass
        elif 'COMMIT' in tokens:
            self.table_map_bakup = self.table_map
        elif 'ROLLBACK' in tokens:
            self.table_map = self.table_map_bakup
        else:
            raise NotImplementedError(f'TBD: {ast}')
        return rows

    def transaction(self, ast_list: list) -> list:
        self.table_map_bakup = deepcopy(self.table_map)
        rows = []
        try:
            for ast in ast_list:
                rows = self.execute_one(ast)
            self.table_map_bakup = None
        except Exception as e:
            self.table_map = self.table_map_bakup  # ROLLBACK
            logger.exception(e)
            raise e
        return rows

    def select(self, ast) -> list:
        return []

    def create_table(self, ast: Expression) -> list:
        raise NotImplementedError()

    def create_sequence(self, ast: Expression) -> list:
        raise NotImplementedError()

    def alter_table(self, ast: Expression) -> list:
        raise NotImplementedError()

    def compile_sql(self, stmt: str, params: dict) -> str:
        raise NotImplementedError()

    def regroup_execution_steps(self, ast_list: list) -> list:
        execution_steps = []
        transactions = []
        for ast in ast_list:
            sql = ast.sql().upper()
            tokens = [k for k in self.KEYWORDS if k in sql]
            if 'BEGIN' in tokens and not transactions:
                transactions.append(ast)
            elif 'COMMIT' not in tokens and transactions:
                transactions.append(ast)
            elif 'COMMIT' in tokens:
                transactions.append(ast)
                execution_steps.append(transactions)
                transactions = []
            else:
                execution_steps.append(ast)
        return execution_steps


class PostgresDBMocker(BaseRelationalDBMocker):
    DIALECT = 'postgres'
    KEYWORDS = sorted(Postgres.Tokenizer.KEYWORDS.keys()) + [
        'SEQUENCE',
    ]

    def create_table(self, ast: Expression) -> list:
        col_names, dtypes = [], []
        for c in ast.find_all(exp.ColumnDef):
            col_names.append(c.name)
            datatype = c.find(exp.DataType).this.name
            dtypes.append(PG_DTYPE_TO_PANDAS.get(datatype))
        tablename = ast.find(exp.Table).this.name
        df = pd.DataFrame([], columns=col_names)
        self.table_map[tablename] = df
        return [{'msg': f'Created table {tablename}'}]

    def create_sequence(self, ast: Expression) -> list:
        from py_db_mocker.postgres_parser import PgParseCreateSequence
        seq = PgParseCreateSequence(sql=ast.sql())
        if self.sequence_map.get(seq.name):
            raise NameError(f'Sequence name already existed: {seq.name}')
        self.sequence_map[seq.name] = seq
        return [{'msg': f'Created sequence {seq.name}'}]

    def alter_table(self, ast: Expression) -> list:
        from py_db_mocker.postgres_parser import PgParseAlterTable
        alt = PgParseAlterTable(sql=ast.sql())
        return [{'msg': f'Altered table {alt.tablename}'}]

    def compile_sql(self, stmt: str, params: dict = None) -> str:
        """ EXPERIMENTAL FUNC: NOT TO BE USED ON PRODUCTION QUERY """
        if not params:
            return stmt
        sql = ''
        try:
            sql, params = str(stmt), params or {}
            for k, v in params.items():
                if not v:
                    continue
                if isinstance(v, (list, tuple)):
                    vlist = ', '.join([f"'{x}'" if isinstance(x, str) else str(x) for x in v])
                    sql = sql.replace(f':{k}', vlist)
                elif isinstance(v, (int, float)):
                    sql = sql.replace(f':{k}', str(v))
                else:
                    sql = sql.replace(f':{k}', f"'{v}'")
        except Exception as e:
            logger.exception(e)
        return sql


def main():
    sql = open('./tests/sql_examples/postgres/create_002.sql').read()
    db = PostgresDBMocker()
    db.execute(sql)
    db.execute(''' SELECT * FROM email_email WHERE id = 1; ''')


if __name__ == '__main__':
    main()
