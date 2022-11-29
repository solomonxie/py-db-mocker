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
from collections import OrderedDict

import pandas as pd
from sqlglot import parse
from sqlglot.dialects import Postgres
from sqlglot.expressions import Expression

from common_utils import is_sublist

logger = logging.getLogger(__name__)


class BaseRelationalDBMocker:
    DIALECT = None
    KEYWORDS = []

    def __init__(self, *args, **kwargs):
        self.table_map = OrderedDict()
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
                rows = self.execute_step(step)
        return rows

    def execute_step(self, ast: Expression) -> list:
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
                rows = self.execute_step(ast)
            self.table_map_bakup = None
        except Exception as e:
            self.table_map = self.table_map_bakup  # ROLLBACK
            logger.exception(e)
            raise e
        return rows

    def select(self, ast) -> list:
        return []

    def create_table(self, ast: Expression) -> list:
        return []

    def create_sequence(self, ast: Expression) -> list:
        return []

    def alter_table(self, ast: Expression) -> list:
        return []

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
    KEYWORDS = sorted(Postgres.Tokenizer.KEYWORDS.keys()) + ['SEQUENCE']

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
