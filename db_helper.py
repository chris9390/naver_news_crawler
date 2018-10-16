#!/usr/bin/python
# -*- coding: utf-8 -*-

import time


class DB_Helper:
    def __init__(self, conn):
        self.conn = conn
        self.print_sql_len = 200

    def now(self):
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def print_sql(self, sql):
        if len(sql) >= self.print_sql_len:
            sql_print = sql[:self.print_sql_len] + '...'
        else:
            sql_print = sql

        print("SQL:", sql_print)





    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================




    def insert_new_text(self, dict):
        c = self.conn.cursor()

        sql = "INSERT INTO SentenceTable (sent_id, sent_original, sent_is_added, ArticleTable_article_id) " \
              "VALUES ('%s', '%s', '%s', '%s')" % (dict['sent_id'], dict['sent_original'], dict['sent_is_added'], dict['ArticleTable_article_id'])

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows inserted: %d" % c.rowcount)
        return



    def insert_crawled_article(self, url, title, upl_date, col_date, aid, raw, sid1, sid2):
        c = self.conn.cursor()


        sql = "INSERT INTO ArticleTable (article_url, article_title, article_uploaded_date, article_collected_date, article_aid, article_raw, article_sid1, article_sid2)" \
              "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (url, title, upl_date, col_date, aid, raw, sid1, sid2)


        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows inserted: %d" % c.rowcount)
        return


    def insert_crawled_sentence(self, original, article_id):
        c = self.conn.cursor()


        sql = "INSERT INTO SentenceTable (sent_original, ArticleTable_article_id) VALUES ('%s', '%s')" % (original, article_id)


        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows inserted: %d" % c.rowcount)
        return


    # ===============================================================================================

    def select_one_column_from_table(self, column_name, table_name):
        c = self.conn.cursor()

        sql = "SELECT %s FROM %s" % (column_name, table_name)

        c.execute(sql)

        rows = c.fetchall()
        return rows


    def select_every_rows_from_table(self, table_name):
        c = self.conn.cursor()

        sql = "SELECT * FROM %s" % table_name

        c.execute(sql)

        rows = c.fetchall()
        return rows



    def select_every_rows_from_sentence_by_id(self, id):
        c = self.conn.cursor()

        sql = "SELECT * FROM SentenceTable WHERE ArticleTable_article_id= %s" % id
        c.execute(sql)

        rows = c.fetchall()
        return rows


    def select_data_from_table_by_something(self, column_name, table_name, where_col_name, where_val):
        c = self.conn.cursor()

        sql = "SELECT %s as data FROM %s WHERE %s = %s" % (column_name, table_name, where_col_name, where_val)

        c.execute(sql)

        row = c.fetchone()['data']
        return row




    def select_largest_sent_id(self):
        c = self.conn.cursor()

        sql = "SELECT sent_id as id FROM SentenceTable ORDER BY sent_id DESC LIMIT 1"

        c.execute(sql)

        row = c.fetchone()['id']
        return row



    def select_smallest_sent_id_of_one_article(self, article_id):
        c = self.conn.cursor()

        sql = "SELECT sent_id as id FROM SentenceTable WHERE ArticleTable_article_id = %s ORDER BY sent_id ASC LIMIT 1" % article_id

        c.execute(sql)

        row = c.fetchone()['id']
        return row


    def select_sent_id(self, article_id):
        c = self.conn.cursor()

        sql = "SELECT sent_id FROM SentenceTable WHERE ArticleTable_article_id = %s" % article_id

        c.execute(sql)

        rows = c.fetchall()
        return rows



    def select_every_rows_including_text_from_table(self, table_name, text):
        c = self.conn.cursor()


        sql = "SELECT * FROM %s WHERE (sent_original LIKE '%%%s%%' AND sent_confirm = 0) OR " \
                                        "(sent_converted LIKE '%%%s%%' AND sent_confirm = 1)" % (table_name, text, text)

        c.execute(sql)

        rows = c.fetchall()
        return rows

    # ===============================================================================================

    def update_sent_converted(self, text, id):
        c = self.conn.cursor()



        if "'" in text:
            text = text.replace("'", "''")
        sql = 'UPDATE SentenceTable SET sent_converted = \'%s\' WHERE sent_id = %s' % (text, id)



        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)



    def update_sent_modified_date(self, id):
        c = self.conn.cursor()


        current = self.now()
        sql = "UPDATE SentenceTable SET sent_modified_date = '%s' WHERE sent_id = %s" % (current, id)

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)




    def update_sent_confirm(self, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_confirm = 1 WHERE sent_id = %s" % id

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)




    def update_sent_ambiguity(self, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_ambiguity = 1 WHERE sent_id = %s" % id

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)




    def update_sent_converted_count(self, converted_count, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_converted_count = %s WHERE sent_id = %s" % (converted_count, id)

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)




    def update_crawled_article(self, url, title, upl_date, col_date, aid, raw, sid1, sid2):
        c = self.conn.cursor()


        sql = "UPDATE ArticleTable SET article_url = '%s', article_title = '%s', article_uploaded_date = '%s', article_collected_date = '%s', article_raw = '%s', article_sid1 = '%s', article_sid2 = '%s'" \
              " WHERE article_aid = '%s'" % (url, title, upl_date, col_date, raw, aid, sid1, sid2)


        #self.print_sql(sql)
        print(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)



    def update_article_sent_count(self, article_id, cnt):
        c = self.conn.cursor()

        sql = "UPDATE ArticleTable SET article_sent_count = %s WHERE article_id = %s" % (cnt, article_id)

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)



    def update_crawled_sentence(self, updated_sent, article_id, sent_id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_original = '%s', sent_converted ='', sent_modified_date = '0000-00-00 00:00:00', sent_confirm = 0, sent_ambiguity = 0, sent_converted_count = 0, sent_is_added = 0" \
              " WHERE ArticleTable_article_id = %s AND sent_id = %s" % (updated_sent, article_id, sent_id)


        print(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)



    # ===============================================================================================


    def delete_sent_by_sentence_id(self, id):
        c = self.conn.cursor()
        sql = "DELETE FROM SentenceTable WHERE sent_id = %s" % (id)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows deleted: %d" % c.rowcount)
        return


    def delete_sent_by_article_id(self, id):
        c = self.conn.cursor()
        sql = "DELETE FROM SentenceTable WHERE ArticleTable_article_id = %s" % (id)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows deleted: %d" % c.rowcount)
        return

    # ===============================================================================================


    def call_board(self, page, per_page):

        c = self.conn.cursor()

        # 1페이지는 0부터 시작, 2페이지는 10부터 시작...
        limit_start = per_page * (page - 1)

        sql = "SELECT ST.*, AT.article_id, AT.article_collected_date FROM SentenceTable as ST left join ArticleTable as AT on ST.ArticleTable_article_id = AT.article_id"
        sql += " LIMIT %s,%s" % (limit_start, per_page)

        c.execute(sql)

        rows = c.fetchall()
        return rows



    def call_board_search(self, page, per_page, text):

        c = self.conn.cursor()

        # 1페이지는 0부터 시작, 2페이지는 10부터 시작...
        limit_start = per_page * (page - 1)

        sql = "SELECT ST.*, AT.article_id, AT.article_collected_date FROM SentenceTable as ST left join ArticleTable as AT on ST.ArticleTable_article_id = AT.article_id"
        sql += " WHERE (sent_original LIKE '%%%s%%' AND sent_confirm = 0) OR (sent_converted LIKE '%%%s%%' AND sent_confirm = 1)" % (text, text)
        sql += " LIMIT %s,%s" % (limit_start, per_page)

        c.execute(sql)

        rows = c.fetchall()
        return rows


    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
    # ===============================================================================================
