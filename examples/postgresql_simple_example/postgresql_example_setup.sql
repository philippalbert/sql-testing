CREATE TABLE PEOPLE
(
    contact_id INTEGER PRIMARY KEY,
    first_name TEXT    NOT NULL,
    last_name  TEXT    NOT NULL,
    email      TEXT    NOT NULL UNIQUE,
    age        INTEGER NOT NULL,
    country_id INTEGER NOT NULL,
    is_famous  BOOLEAN NOT NULL

);

INSERT INTO PEOPLE
VALUES (1, 'Philipp', 'Albert', 'something@somthing', 31, 49, FALSE),
       (2, 'Hans', 'Zimmer', 'hans.zimmer', 55, 1, TRUE),
       (3, 'Michael', 'Jordan', 'mj@gmail.com', 52, 1, TRUE),
       (4, 'Juergen', 'Schmidhuber', 'js@somehing', 64, 49, TRUE),
       (5, 'Johanna', 'Suchner', 'j.s@gmx.de', 31, 49, FALSE)
;

CREATE TABLE COUNTRIES
(
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO COUNTRIES
VALUES (1, 'USA'),
       (49, 'Germany')
;
