beGin;


CREAte  Table  users (
    id integer NOT NULL,
    name varchar(255) NOT NULL,
    email_id integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

CREAte  Table  email_email (
    id integer NOT NULL,
    email varchar(255) NOT NULL,
    email_hash varchar(255),
    domain_id varchar(511),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

crEAte  seqUENce  email_email_id_seq
    INCREMENT BY 1
    MINVALUE 10
    NO MAXVALUE
    CACHE 1
    START WITH 100
;

ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);


ALTER  TAbLE IF EXiSTS OnLY  email_email ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);


INSERT INTO email_email(id, email, created_at) VALUES(
    1, 'a@a.com', '2022-12-13 17:02:40'
);


INSERT INTO email_email(email, domain_id) VALUES(
    ('a@aaa.com', 711),
    ('b@bbb.com', 712)
);

INSERT INTO users(id, name, email_id) VALUES(
    (1, 'Jason(a)', 711),
    (2, 'Tom(b)', 712)
);


SELECT
    id, email, 1
FROM email_email
WHERE
    1=1
    AND id > 1
;

 coMMit ;
