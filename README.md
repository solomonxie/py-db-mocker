# py-db-mocker

Mock Relational Database server with In-memory SQL interpreter engine, simplify unit test and reduce the external dependency on actual servers.


## Roadmap

`sqlglot` based:
- [ ] `SELECT`
    - [ ] `BASIC SELECT`
    - [ ] `JOIN TABLES`
    - [ ] `WINDOW FUNCTION: RANK`
- [ ] `CREATE TABLE`
    - [x] `SET COLUMN NAMES`
    - [x] `SET COLUMN BASIC DATA TYPES`
    - [ ] `SET COLUMN ADVANCED DATA TYPES`


`State Machine Parser` based:
- [x] `CREATE SEQUENCE`
- [ ] `ALTER TABLE`
    - [x] `SET COLUMN DEFAULT`
    - [x] `ADD COLUMN CONSTRAINT`
    - [ ] TODO
