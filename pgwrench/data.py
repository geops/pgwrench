import psycopg2.extras


def generate_upserts(db, options, tables):
    """"""

    if type(tables) in (list, tuple):
        cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for table in tables:
            print "\n-- start: table %s\n" % table

            # get table columns without colums - simply let errors caused by wron
            # table names propagete to the user
            cur.execute("""select pa.attname, coalesce(pks.is_pk, false)::boolean as is_pk
                from pg_attribute pa
                left join (
                select conkey[i], true::boolean as is_pk from (
                    select conkey, generate_series(array_lower(conkey,1), array_upper(conkey,1)) as i
                        from pg_constraint
                        where conrelid=%s::text::regclass::oid and contype = 'p'
                    ) pkcon
                ) pks on pks.conkey=pa.attnum
                where attrelid=%s::text::regclass::oid and attnum>0 and not attisdropped""", (table, table))
            rows = cur.fetchall()
            pks = []
            nonpks = []
            for row in rows:
                if row['is_pk']:
                    pks.append(row['attname'])
                else:
                    nonpks.append(row['attname'])

            if len(pks) == 0:
                raise Exception("table %s does not have a primary key" % table)


            cur.execute("select * from %s;" % (table,))
            for row in cur.fetchall():

                # insert statement
                args = []
                set_columns = []
                where_columns = []
                for nonpk in nonpks:
                    args.append(row[nonpk])
                    set_columns.append(nonpk)
                for pk in pks:
                    args.append(row[pk])
                    set_columns.append(pk)
                for pk in pks:
                    args.append(row[pk])
                    where_columns.append(pk+"=%s")

                raw_sql = "insert into %s (%s) select %s from (select count(*) as c from %s where %s) as foo where foo.c = 0;" % (table, ', '.join(set_columns),
                        ', '.join(len(set_columns) * ["%s"]), table, ' and '.join(where_columns))
                print cur.mogrify(raw_sql, args)


                # update statement
                args = []
                update_columns = []
                where_columns = []
                for nonpk in nonpks:
                    args.append(row[nonpk])
                    update_columns.append(nonpk+"=%s")
                for pk in pks:
                    args.append(row[pk])
                    where_columns.append(pk+"=%s")
                raw_sql = "update %s set %s where %s;" % (table, ', '.join(update_columns), ' and '.join(where_columns))
                print cur.mogrify(raw_sql, args)
                print

            print "\n-- end: table %s\n" % table

