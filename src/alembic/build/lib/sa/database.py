import psycopg2

"""
Defines the database class that gives functions that allow certain functions
on the database. It is important to seperate the actual implementation of the db
from the general code.

Two gotchas:
     -Try/Except expected in __init__() for custom handling
     -This class assumes the tables are already present
"""


class Database:
    def __init__(self):
        self.connect = psycopg2.connect(
            ("dbname='stockanalyzer' user='root'"
             "host='localhost' password='toor'"))

        self.cursor = self.connect.cursor()

    def _create_fillable_str(self, height, length):

        base_str = '(' + ','.join('%s' for _ in range(length)) + ')'
        if height > 1:
            row_str = base_str + ','
            rows_str = row_str * (height - 1) + base_str
        else:
            rows_str = base_str
        
        return rows_str
    

    def insert_into(self, table, fields, rows, multiple=True, unique_conflict=False):
        fields_str = '"' + '","'.join(str(e) for e in fields) + '"'
        unique_conflict = " ON CONFLICT DO NOTHING" if unique_conflict else ""

        if multiple:
            rows_str = self._create_fillable_str(len(rows), len(rows[0]))
            flat_rows = tuple(i for sublist in rows for i in sublist)
        else:
            rows_str = self._create_fillable_str(1, len(rows))
            flat_rows = rows

        cmd = ("INSERT INTO {} "
               "({}) "
               "VALUES {}{}").format(table, fields_str,
                                     rows_str, unique_conflict)

        self.cursor.execute(cmd, flat_rows)
        self.connect.commit()

    def update(self, table, fields, cols, where, wherevals):
        cols_str = self._create_fillable_str(1, len(cols))
        fields_str = '("' + '","'.join(str(e) for e in fields) + '")'
        cmd = ("UPDATE {} "
               "SET {} = {} "
               "WHERE {}").format(table, fields_str, cols_str, where)

        self.cursor.execute(cmd, cols + wherevals)
        self.connect.commit()

    def insert_dic_into(self, table, dic):
        cols = list(dic.keys())
        vals = list(dic.values())

        self.insert_into(table, cols, vals, multiple=False)
        

    def empty_table(self, table):
        cmd = "TRUNCATE {}".format(table)
        self.cursor.execute(cmd)
        self.connect.commit()

    def get_col_names(self, table):
        cmd = ("SELECT column_name "
               "FROM information_schema.columns "
               "WHERE table_catalog='stockanalyzer' "
               "AND table_name='{}' ORDER BY ordinal_position;").format(table)
        self.cursor.execute(cmd)
        col_names = [title[0] for title in self.cursor.fetchall()]
        return col_names

    def select(self, cols, table, where="", vals=[], fetch='all', unroll=False):
        if where:
            where = " WHERE " + where

        cmd = ("SELECT {} "
               "FROM {}{}").format(
                   cols,
                   table,
                   where
               )
        
        self.cursor.execute(cmd, vals)

        if fetch == 'all':
            records = self.cursor.fetchall()
            if unroll and len(records):
                records = [x[0] for x in records]
        elif fetch == 'one':
            records = self.cursor.fetchone()
            if unroll and len(records):
                records = records[0]

        return records

    def exists(self, table, where, vals):
        cmd = ("SELECT EXISTS(SELECT 1 FROM "
               "{} where {})").format(table, where, vals)

        self.cursor.execute(cmd, vals)
        return self.cursor.fetchone()[0]

    def create_conditional_string(self, vals, sep="AND"):
        where = " {} ".format(sep).join("{}=%s".format(v) for v in vals)
        return where

    def destroy(self):
        self.cursor.close()
        self.connect.close()

if __name__ == "__main__":
    db = Database()
    x = db.select("ticker, dateoflisting", "listings", where="exchange = %s", vals=["TSX"])
    print(x)
    db.destroy()
