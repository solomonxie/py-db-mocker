
from py_db_mocker.postgres_parser import PgParseAlterTable
from py_db_mocker.postgres_parser import PgParseCreateSequence


def test_alter_table_add_constraint():
    sql = """
        ALTER  TAbLE IF EXiSTS OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);
    """
    cs = PgParseAlterTable(sql)
    assert cs.constraint_map['email_email.id'] == {'type': 'primary_key', 'value': 'id'}


def test_alter_table_set_default_value():
    seq = PgParseCreateSequence(start=12, increment=3)
    seq.next_value()
    sql = """
        ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);
    """
    dv = PgParseAlterTable(sql, sequence_map={'email_email_id_seq': seq})
    assert dv.default_value_map.get('email_email.id') == 15


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
    seq = PgParseCreateSequence(sql)
    assert seq.name == 'email_email_id_seq'
    assert seq.start == 10
    assert seq.increment == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1
