-- Role: "termbinduser"
CREATE ROLE termbinduser LOGIN
  ENCRYPTED PASSWORD 'md5c6c5e55d46f1eab8d5ff30f3f502688e'
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
GRANT termbindgroup TO termbinduser;

-- Role: "termbindgroup"
CREATE ROLE termbindgroup
  NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;

GRANT termbindgroup TO termbinduser;