beGin;

CREAte  Table  email_email (
    id integer NOT NULL,
    email varchar(255) NOT NULL,
    email_hash varchar(255) NOT NULL,
    domain_id varchar(511) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

crEAte  seqUENce  email_email_id_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 10
    NO MAXVALUE
    CACHE 1;

ALtER   TAbLE   ONlY email_email ALTER COLUMN id SET DEFAULT nextval('email_email_id_seq'::regclass);


ALTER  TAbLE OnLY  email_email
    ADD CONSTRAINT email_email_pkey PRIMARY KEY (id);

 coMMit ;
