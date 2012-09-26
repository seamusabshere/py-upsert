import upsert
import itertools

class MergeFunction(upsert.MergeFunction):
    @classmethod
    def call_template(cls, controller, name, selector_keys, setter_keys):
        return controller.fill_ident_placeholders('CALL %s', (name,)) + '(' + ','.join(['?']*(len(selector_keys) + len(setter_keys))) + ')'

    def execute(self, row):
        first_try = True
        try:
            self.controller.execute(self.call_template, (row.selector.values() + row.setter.values()))
        except Exception as e:
            if first_try and str(type(e)) == "<class '_mysql_exceptions.OperationalError'>" and e.args[0] == 1305:
                first_try = False
                print '[upsert] Trying to recreate function {0}'.format(self.name)
                self.create_or_replace()
            else:
                raise e

    def drop(self):
        self.controller.execute3('DROP PROCEDURE IF EXISTS %s', (self.name,), ())

    # http://stackoverflow.com/questions/11371479/how-to-translate-postgresql-merge-db-aka-upsert-function-into-mysql/
    def create(self):
        self.controller.execute3('SHOW COLUMNS FROM %s', (self.controller.table_name,), ())
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

        # table name
        chunks.append('%s')
        idents.append(self.controller.table_name)

        # condition in COUNT
        selector_sql = ' AND '.join(['%s = %s']*len(self.selector_keys))
        selector_idents = list(itertools.chain.from_iterable([k, k+'_SEL'] for k in self.selector_keys))
        chunks.append(selector_sql)
        idents.extend(selector_idents)

        # update sql
        chunks.append('%s')
        idents.append(self.controller.table_name)

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

        a = """
            CREATE PROCEDURE %s(%s)
            BEGIN
                DECLARE done BOOLEAN;
                REPEAT
                    BEGIN
                        -- If there is a unique key constraint error then 
                        -- someone made a concurrent insert. Reset the sentinel
                        -- and try again.
                        DECLARE ER_DUP_UNIQUE CONDITION FOR 23000;
                        DECLARE ER_INTEG CONDITION FOR 1062;
                        DECLARE CONTINUE HANDLER FOR ER_DUP_UNIQUE BEGIN
                            SET done = FALSE;
                        END;
                        
                        DECLARE CONTINUE HANDLER FOR ER_INTEG BEGIN
                            SET done = TRUE;
                        END;

                        SET done = TRUE;
                        SELECT COUNT(*) INTO @count FROM %s WHERE %s;
                        -- Race condition here. If a concurrent INSERT is made after
                        -- the SELECT but before the INSERT below we'll get a duplicate
                        -- key error. But the handler above will take care of that.
                        IF @count > 0 THEN 
                            -- UPDATE table_name SET b = b_SET WHERE a = a_SEL;
                            UPDATE %s SET %s WHERE %s;
                        ELSE
                            -- INSERT INTO table_name (a, b) VALUES (k, data);
                            INSERT INTO %s (%s) VALUES (%s);
                        END IF;
                    END;
                UNTIL done END REPEAT;
            END
        """ % tuple(chunks)
        self.controller.execute3(a, tuple(idents), ())

class Mysql:
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

    def quote_ident(self, str, errors="strict"):
        # unless it's in ANSI mode
        return '`' + str + '`'
