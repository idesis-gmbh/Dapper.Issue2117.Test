DROP TABLE IF EXISTS parent CASCADE;

CREATE TABLE parent (
	id uuid NOT NULL PRIMARY KEY
);

DROP TABLE IF EXISTS child CASCADE;

CREATE TABLE child (
	id uuid NOT NULL PRIMARY KEY,
	parent_id uuid NOT NULL
);

ALTER TABLE child ADD FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE CASCADE;

CREATE INDEX ON child(parent_id);

insert into parent(id)
select gen_random_uuid()
from generate_series(1, 1000);

insert into child(id, parent_id)
select gen_random_uuid(), id 
from parent 
cross join generate_series(1, 10);
