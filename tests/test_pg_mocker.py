
from py_db_mocker.postgres_mocker import PostgresAlterTable
from py_db_mocker.postgres_mocker import PostgresCreateSequence


def test_alter_table_add_constraint():
    sql = """
        ALTER  TAbLE IF EXiSTS OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);
    """
    cs = PostgresAlterTable(sql)
    assert cs.constraint_map['email_email.id'] == {'type': 'primary_key', 'value': 'id'}


def test_alter_table_set_default_value():
    sql = """
        ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);
    """
    dv = PostgresAlterTable(sql)
    assert dv.default_value_map.get('email_email.id') is not None


def test_create_sequence():
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
    assert seq.name == 'email_email_id_seq'
    assert seq.start_with == 10
    assert seq.increment_by == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1
