import upsert
import itertools

class MergeFunction(upsert.MergeFunction):
    @classmethod
    def call_template(cls, controller, name, selector_keys, setter_keys):
        return controller.fill_ident_placeholders('SELECT %s', (name,)) + '(' + ','.join(['?']*(len(selector_keys) + len(setter_keys))) + ')'

    def execute(self, row):
        first_try = True
        try:
            self.controller.execute(self.call_template, (row.selector.values() + row.setter.values()))
        except Exception as e:
            if first_try and str(type(e)) == "<class 'psycopg2.ProgrammingError'>" and e.pgcode == 42883:
                first_try = False
                print '[upsert] Trying to recreate function {0}'.format(self.name)
                self.create_or_replace()
            else:
                raise e

    def drop(self):
        pass

    # http://stackoverflow.com/questions/11371479/how-to-translate-postgresql-merge-db-aka-upsert-function-into-mysql/
    def create(self):
        # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
        self.controller.execute3("""
            SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
            FROM pg_attribute a LEFT JOIN pg_attrdef d
            ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            WHERE a.attrelid = '%s'::regclass
            AND a.attnum > 0 AND NOT a.attisdropped
        """, (self.controller.table_name,), ())
        name_and_type = { r[0]: r[1] for r in self.controller.cursor.fetchall() }
        
        chunks = []
        idents = []

        # function name
        chunks.append('%s')
        idents.append(self.name)

        # args
        chunks.append(','.join('%s ' + name_and_type[k] for k in (self.selector_keys + self.setter_keys)))
        idents.extend(k+'_SEL' for k in self.selector_keys)
        idents.extend(k+'_SET' for k in self.setter_keys)

        # update sql
        chunks.append('%s')
        idents.append(self.controller.table_name)

        selector_sql = ' AND '.join(['%s = %s']*len(self.selector_keys))
        selector_idents = list(itertools.chain.from_iterable([k, k+'_SEL'] for k in self.selector_keys))
        setter_sql = ','.join(['%s = %s']*len(self.setter_keys))
        setter_idents = list(itertools.chain.from_iterable([k, k+'_SET'] for k in self.setter_keys))
        chunks.append(setter_sql)
        idents.extend(setter_idents)

        chunks.append(selector_sql)
        idents.extend(selector_idents)

        # insert sql
        chunks.append('%s')
        idents.append(self.controller.table_name)

        chunks.append(','.join(['%s']*len(self.setter_keys)))
        idents.extend(self.setter_keys)

        chunks.append(','.join(['%s']*len(self.setter_keys)))
        idents.extend(k+'_SET' for k in self.setter_keys)

        # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
        a = """
            CREATE OR REPLACE FUNCTION %s(%s) RETURNS VOID AS
            $$
            DECLARE
              first_try INTEGER := 1;
            BEGIN
              LOOP
                  -- first try to update the key
                  UPDATE %s SET %s
                      WHERE %s;
                  IF found THEN
                      RETURN;
                  END IF;
                  -- not there, so try to insert the key
                  -- if someone else inserts the same key concurrently,
                  -- we could get a unique-key failure
                  BEGIN
                      INSERT INTO %s(%s) VALUES (%s);
                      RETURN;
                  EXCEPTION WHEN unique_violation THEN
                      -- seamusabshere 9/20/12 only retry once
                      IF (first_try = 1) THEN
                        first_try := 0;
                      ELSE
                        RETURN;
                      END IF;
                      -- Do nothing, and loop to try the UPDATE again.
                  END;
              END LOOP;
            END;
            $$
            LANGUAGE plpgsql;
        """ % tuple(chunks)
        self.controller.execute3(a, tuple(idents), ())

class Postgresql(upsert.AnsiIdent):
    row_class = upsert.Row

    def __init__(self, controller):
        self.controller = controller
        self.buffer = []

    def ready(self):
        if len(self.buffer) == 0:
            return
        first_row = self.buffer.pop()
        merge_function = MergeFunction.lookup(self.controller, tuple(first_row.selector.keys()), tuple(first_row.setter.keys()))
        merge_function.execute(first_row)

    # expects placeholders as ?
    def execute(self, template, values = ()):
        template = template.replace('?', '%s')
        cur = self.controller.cursor
        cur.execute(template, values)
        cur.connection.commit()