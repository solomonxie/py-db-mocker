

class PostgresSequence:
    KEYWORDS = [
        'START WITH',
        'INCREMENT BY',
        'NO',
        'MINVALUE',
        'MAXVALUE',
    ]

    def __init__(self, sql):
        upper_sql = str(sql).upper()
        parts = [s.strip() for s in upper_sql.split()]
        if not parts[0:1] != ['CREATE', 'SEQUENCE']:
            raise NotImplementedError(f'TBD: {sql}')
        self.name = None
        self.value = None
        self.start_with = 0
        self.increment_by = None
        self.max_value = None
        self.min_value = None
        self.cache = None
        last_token = None
        for i, s in enumerate(parts):
            last_token = parts[i - 1]
            last_last_token = parts[i - 2]
            next_token = parts[i + 1] if i + 1 < len(parts) else None
            if last_token == 'SEQUENCE':
                idx = upper_sql.index(s)
                self.name = sql[idx: idx+len(s)]
            elif last_last_token == 'START' and last_token == 'WITH':
                self.start_with = int(s)
            elif last_last_token == 'INCREMENT' and last_token == 'BY':
                self.increment_by = int(s)
            elif s == 'MINVALUE' and next_token and last_token != 'NO':
                self.min_value = int(next_token)
            elif s == 'MINVALUE' and last_token == 'NO':
                self.min_value = None
            elif s == 'MAXVALUE' and next_token and last_token != 'NO':
                self.max_value = int(next_token)
            elif s == 'MAXVALUE' and last_token == 'NO':
                self.max_value = None
            elif s == 'CACHE' and next_token and last_token != 'NO':
                self.cache = int(next_token)
            elif s == 'CACHE' and last_token == 'NO':
                self.cache = None
            else:
                pass
        return

    def next_val(self):
        pass


def main():
    sql = """
    crEAte  seqUENce  email_email_id_seq
        START WITH 10
        INCREMENT BY 1
        NO MINVALUE
        MAXVALUE 1000
        CACHE 1;
    """
    seq = PostgresSequence(sql)
    assert seq.name == 'email_email_id_seq'
    assert seq.start_with == 10
    assert seq.increment_by == 1
    assert seq.min_value is None
    assert seq.max_value == 1000
    assert seq.cache == 1


if __name__ == '__main__':
    main()
