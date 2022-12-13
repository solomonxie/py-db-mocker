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
    user_id integer NOT NULL,
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
    (1, 'a@a.com', '2022-12-13 17:02:40')
);


INSERT INTO email_email(id, email, created_at) VALUES(
    2, 'b@b.com', '2022-12-14 17:02:40'
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
    id,
    user_id,
    email
FROM (
    SELECT user_id FROM users
) AS email_email
where
    user_id > 0
;


SELECT
    count(id) as user_email_count,
    user_id
FROM email_email
where
    user_id > 0
GROUP BY
    user_id
;


SELECT
    A.id AS email_id,
    A.email,
    NULL AS user_name,
    B.id AS user_id,
    1
FROM email_email as A
JOIN users as B ON
    1=1
    AND email_email.user_id = users.id
WHERE
    1=1
    AND A.id > 1
;

 coMMit ;
