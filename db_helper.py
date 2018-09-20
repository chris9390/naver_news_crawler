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

    def generate_insert_key_values(self, values, without_date_fields=False):
        """
        SQL Insert 문에 필요한 key와 values를 생성
        :param values:
        :return:
        """
        insert_cols = ''
        insert_string = ''

        if not without_date_fields:
            values['created_at'] = self.now()
            values['updated_at'] = self.now()

        for key, value in values.items():
            if value is not None:
                insert_cols += '`%s`, ' % key
                insert_string += '\"%s\", ' % value

        insert_cols = insert_cols.strip()[:-1]
        insert_string = insert_string.strip()[:-1]

        return insert_cols, insert_string

    def generate_update_key_values(self, values):
        """
        SQL Update 문에 필요한 key와 values를 생성
        :param values:
        :return:
        """
        update_string = ''

        values['updated_at'] = self.now()

        for key, value in values.items():
            if value == None or len(str(value)) == 0:
                value = 'null'
                update_string += '`%s` = %s, ' % (key, value)
            else:
                update_string += '`%s` = \'%s\', ' % (key, value)

        update_string = update_string.strip()[:-1]

        return update_string




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



    def insert_crawled_article(self, url, title, upl_date, col_date, good, warm, sad, angry, want, aid, raw):
        c = self.conn.cursor()


        sql = "INSERT INTO ArticleTable (article_url, article_title, article_uploaded_date, article_collected_date, article_good, article_warm, article_sad, article_angry, article_want, article_aid, article_raw)" \
              "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (url, title, upl_date, col_date, good, warm, sad, angry, want, aid, raw)


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
        return


    def update_sent_modified_date(self, id):
        c = self.conn.cursor()


        current = self.now()
        sql = "UPDATE SentenceTable SET sent_modified_date = '%s' WHERE sent_id = %s" % (current, id)

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return



    def update_sent_confirm(self, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_confirm = 1 WHERE sent_id = %s" % id

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return



    def update_sent_ambiguity(self, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_ambiguity = 1 WHERE sent_id = %s" % id

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return



    def update_sent_converted_count(self, converted_count, id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_converted_count = %s WHERE sent_id = %s" % (converted_count, id)

        self.print_sql(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return



    def update_crawled_article(self, url, title, upl_date, col_date, good, warm, sad, angry, want, aid, raw):
        c = self.conn.cursor()


        sql = "UPDATE ArticleTable SET article_url = '%s', article_title = '%s', article_uploaded_date = '%s', article_collected_date = '%s', article_good = %s, article_warm = %s, article_sad = %s, article_angry = %s, article_want = %s, article_raw = '%s'" \
              " WHERE article_aid = '%s'" % (url, title, upl_date, col_date, good, warm, sad, angry, want, raw, aid)


        #self.print_sql(sql)
        print(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return


    def update_crawled_sentence(self, updated_sent, article_id, sent_id):
        c = self.conn.cursor()

        sql = "UPDATE SentenceTable SET sent_original = '%s', sent_converted ='', sent_modified_date = '0000-00-00 00:00:00', sent_confirm = 0, sent_ambiguity = 0, sent_converted_count = 0, sent_is_added = 0" \
              " WHERE ArticleTable_article_id = %s AND sent_id = %s" % (updated_sent, article_id, sent_id)


        print(sql)

        c.execute(sql)
        self.conn.commit()

        print("Number of rows updated: %d" % c.rowcount)
        return

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
