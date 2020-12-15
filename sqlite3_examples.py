###############################################################################
"""Execute scripts."""
import sqlite3

con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.executescript("""
    create table person(
        fristname,
        lastname,
        age
    );

    create table book(
        title,
        author,
        published
    );

    insert into book(title, author, published)
    values(
        'Dirty pother',
        'El bananero',
        2014
    );
""")

cur.execute("select * from book")
print(cur.fetchall())
con.close()

###############################################################################
""" Using shortcuts methods, executemany to insert multiple records"""
import sqlite3

persons = [
    ("Hugo", "Boos"),
    ("Calvin", "Klein")
]

con = sqlite3.connect(":memory:")

# Create table
con.execute("create table person(first_name, last_name)")

# Fill the table
con.executemany("insert into person(first_name, last_name) values (?, ?)", persons)

# Print the table contents
for row in con.execute("select * from person"):
    print(row)

print("I just deleted", con.execute("delete from person").rowcount, "rows")

con.close()


###############################################################################
"""Create Functions.""""

import sqlite3
import hashlib

def md5sum(t):
    return hashlib.md5(t).hexdigest()

con = sqlite3.connect(":memory:")
con.create_function("md5sum", 1, md5sum)
cur = con.cursor()
cur.execute("select md5(?)", (b"foo",))
print(cur.fetchone()[0])

con.close()
###############################################################################
"""Agregate function."""
import sqlite3


class MySum:
    """Custom sum."""

    def __init__(self):
        """Construct class."""
        self.count = 0

    def step(self, value):
        """Accept num_parameters of aggregate and performs custom procedure."""
        self.count += value

    def finalize(self):
        """Return the final result."""
        return self.count


con = sqlite3.connect(":memory:")
con.create_aggregate("mysum", -1, MySum)
cur = con.cursor()
cur.execute("create table test(i)")
cur.execute("insert into test(i) values (1)")
cur.execute("insert into test(i) values (2)")
cur.execute("select mysum(i) from test")
print(cur.fetchone()[0])

con.close()


###############################################################################
import sqlite3


def collate_reverse(string1, string2):
    """Sort the wrong way."""
    if string1 == string2:
        return 0
    elif string1 < string2:
        return 1
    else:
        return -1


con = sqlite3.connect(":memory:")
con.create_collation("reverse", collate_reverse)

cur = con.cursor()
cur.execute("create table test(x)")
cur.execute("insert into test(x) values ('a')")
cur.execute("insert into test(x) values ('b')")
cur.executemany("insert into test(x) values (?)", [("c",), ("z",)])
cur.execute("select x from test order by x")
for row in cur:
    print(row)

cur.execute("select x from test order by x collate reverse")

# Remove a collation.
con.create_collation("reverse", None)
con.close()


###############################################################################
"""Dictionary."""

import sqlite3


def dict_factory(cursor, row):
    """Return the row in a dictionary way."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


con = sqlite3.connect(":memory:")
con.row_factory = dict_factory
cur = con.cursor()
cur.execute("select 1 as a")
print(cur.fetchone()["a"])

con.close()


###############################################################################
import sqlite3


con = sqlite3.connect(":memory:")
cur = con.cursor()

AUSTRIA = "\xd6sterreich"

# By default, rows are returned as Unicode
cur.execute("select ?", (AUSTRIA, ))
row = cur.fetchone()
assert row[0] == AUSTRIA

# But we can make sqlite3 always return bytestrings ...
con.text_factory = bytes
cur.execute("select ?", (AUSTRIA, ))
row = cur.fetchone()
assert type(row[0]) is bytes
# The bytestrings will be encoded in UTF-8, unless you stored garbage \
# in the database ...
assert row[0] == AUSTRIA.encode("utf-8")

# we can also implement a custom text_factory ...
# here we implement one that appends "foo" to all strings
con.text_factory = lambda x: x.decode("utf-8") + " foo"
cur.execute("select ?", ("dog", ))
row = cur.fetchone()
assert row[0] == "dog foo"

con.close()

###############################################################################
"""Iterdump method, to test it"""
import sqlite3


con = sqlite3.connect('existing_db.db')
with open('dump.sql','w') as f:
    for line in con.iterdump():
        f.write('%s\n' % line)
con.close()

###############################################################################
"""Method backup."""
import sqlite3


def progress(status, remaining, total):
    print(f'Copied {total-remaining} of {total} pages...')

con = sqlite3.connect('existing_db.db')
bck = sqlite3.connect('backup.db')
with bck:
    con.backup(bck, pages=1, progress=progress)
bck.close()
con.close()


###############################################################################
"""Method backup."""
import sqlite3

source = sqlite3.connect('existing_db.db')
dest = sqlite3.connect(':memory:')
source.backup(dest)

###############################################################################
"""Execute method."""
import sqlite3


con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("create table people (name_last, age)")

who = "yeltsion"
age = 32

# This is the mark stile
cur.execute("insert into people values (?, ?)",(who, age))

# This is the named style:
cur.execute("select * from people where name_last=:who and age=:age",{"who": who, "age": age})

print(cur.fetchone())

con.close()

###############################################################################
"""Executemany method."""
import sqlite3

class IterChars:
    def __init__(self):
        self.count == ord('a')

    def __iter__(self):
        return self

    def __next__(self):
        if self.count > ord('z'):
            raise StopIteration
        self.count += 1
        return (chr(self.count -1), ) # this is a 1-tuple

con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("create table characteres(c)")

theIter = IterChars()
cur.executemany("insert into characteres(c) values (?)", theIter)

cur.execute("select c from characteres")
print(cur.fetchall())

con.close()

###############################################################################
"""Executemany method. generator version."""
import sqlite3
import string

def char_generator():
    for c in string.ascii_lowercase:
        yield(c, )

con = sqlite3.connect(":memory:")
cur = con.cursor()

cur.execute("create table characteres(c)")
cur.executemany("insert into characteres(c) values (?)",char_generator())

cur.execute("select * from characteres")
print(cur.fetchall())

con.close()

###############################################################################
"""Provide an example of cursor.connection()"""
import sqlite3

con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.connection == con

###############################################################################
"""Provide an example of row.keys()"""
import sqlite3

# Create table
con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("""
CREATE TABLE STOCKS (
    date text,
    trans text,
    symbol text,
    qty real,
    price real)""")

cur.execute("""
INSERT INTO STOCKS
VALUES ('2020-01-01', 'BUY', 'RHAT', 100, 35.15)
""")
con.commit()

# Plug row
con.row_factory = sqlite3.Row
cur = con.cursor()
cur.execute("select * from stocks")
r = cur.fetchone()
type(r)
tuple(r)
len(r)
r[2]
r.keys()
r['qty']
for member in r:
    print(member)

con.close()

###############################################################################
"""Example of aditional python datatype store"""
import sqlite3


class Point:
    """Abstract a point."""

    def __init__(self, x, y):
        """Define coordantes x and y of a point."""
        self.x, self.y = x, y

    def __conform__(self, protocol):
        """Return result in sqlite3."""
        if protocol == sqlite3.PrepareProtocol:
            return "%2f;%2f" % (self.x, self.y)


con = sqlite3.connect(":memory:")
cur = con.cursor()

p = Point(3.4, -5.5)
cur.execute("select ?", (p, ))
a = cur.fetchone()
print(a)
type(a)

con.close()

###############################################################################
"""Example of adapters type generators"""
import sqlite3

class Point:
    """Represent the basic of geometry."""

    def __init__(self, x, y):
        """Define a point in cartesian plane."""
        self.x, self.y = x, y


def adapt_point(point):
    """Return the x, y point representation."""
    return "%2f;%2f" % (point.x, point.y)


sqlite3.register_adapter(Point, adapt_point)

con = sqlite3.connect(":memory:")
cur = con.cursor()

p = Point(4.5, -3.012)
cur.execute("SELECT ?", (p,))
print(cur.fetchone()[0])

con.close()

###############################################################################
"""Example of default adapters."""
import sqlite3
import datetime
import time


def adapt_datetime(ts):
    """Return the time in unix timestamp."""
    return time.mktime(ts.timetuple())


sqlite3.register_adapter(datetime.datetime, adapt_datetime)

con = sqlite3.connect(":memory:")
cur = con.cursor()

now = datetime.datetime.now()
cur.execute("SELECT ?", (now,))
print(cur.fetchone()[0])
con.close()

####################################################################################
"""Accessing columns by name instead by index."""
import sqlite3

con = sqlite3.Connection(":memory:")
con.row_factory = sqlite3.Row

cur = con.cursor()

cur.execute("select 'pedro' as name, 43 as age")

for row in cur:
    assert row[0] == row['name']
    assert row['name'] == row['NaMe']

con.close()

####################################################################################
"""Using the connection as a context manager."""
import sqlite3

con = sqlite3.connect(":memory:")
con.execute("create table person (id integer primary key, first_name varchar unique)")

# Succesful, con.commit() is called
with con:
    con.execute("insert into person(first_name) values (?)", ("Joe",))

# Error, rollback() is an exception is raised
try:
    with con:
        con.execute("insert into person(first_name) values (?)", ("Joe",))
except sqlite3.IntegrityError:
    print("couldn´t add Joe twice")

for row in con.execute("select * from person"):
    print(row)

con.close()
