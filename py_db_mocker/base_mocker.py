"""
REF: https://media.geeksforgeeks.org/wp-content/uploads/20210920153429/new.png
"""
import sqlglot


class BaseRelationalDBMocker:
    def __init__(self, *args, **kwargs):
        pass

    def execute(self, sql: str):
        pass

    def dql(self, sql: str):
        """ Data Query Language: SELECT """
        pass

    def ddl(self, sql: str):
        """
            Data Definition Language:
            CREATE/DROP/ALTER/TRUNCATE
        """
        pass

    def dml(self, sql: str):
        """
            Data Manipulation Language:
            INSERT/UPDATE/DELETE/CALL/EXPLAIN CALL/LOCK
        """
        pass

    def tcl(self, sql: str):
        """
            Transaction Control Language:
            COMMIT/ROLLBACK/SAVEPOINT/SAVE TRANSACTION/SET CONSTRAINT
        """
        pass

    def dcl(self, sql: str):
        """ Data Control Langauge: GRANT/INVOKE """
        raise NotImplementedError()


def main():
    pass


if __name__ == '__main__':
    main()
