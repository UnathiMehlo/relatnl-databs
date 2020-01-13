import psycopg2
import datetime
import random
import radar
import string
import csv
import argparse

class ProductDatabase:
    def __init__(self):
        try:
            self.con = psycopg2.connect(host="localhost", database="rd_assignment")
            self.cur = self.con.cursor()
        except:
            print("Cannot connect to database")
            
    def add_column(self):
        self.cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL")
        self.con.commit()
        #self.cur.close()


    def id_generator(self):
        chars = string.digits
        return ''.join(random.choice(chars) for _ in range(4))
            
    def desc_generator(self):
        chars = string.ascii_uppercase
        return ''.join(random.choice(chars) for _ in range(6))
    
    def ins_row_data(self):
        c_time =  radar.random_datetime(start='2000-05-24', stop='2013-05-24T23:59:59')
        m_time =  radar.random_datetime(start='2013-05-24', stop='2019-05-24T23:59:59')
        row_data =  "INSERT INTO products (created, modified, description, amount, is_active) VALUES (%s, %s, %s, %s, %s)"
        bool_word = None
        for i in range(20):
            if i % 2 == 0:
                bool_word = True
            else:
                bool_word = False
            vals = [c_time, m_time, self.desc_generator(), self.id_generator(), bool_word]
            self.cur.execute(row_data, vals)
            self.con.commit()
            
    def write_to_csv(self, writefile, records):
        with open(writefile, 'w') as f:
            writer = csv.writer(f, delimiter=',')
            for row in records:
                writer.writerow(row)
  
    def sort_file(self, write_file_1):
        self.cur.execute("SELECT * FROM products ORDER BY amount DESC")
        records = self.cur.fetchall()
        self.write_to_csv(write_file_1, records)
                
                
    def actv_amnt_file(self, write_file_2):
        self.cur.execute("SELECT * FROM products WHERE is_active = 'true' AND amount > 10;")
        records = self.cur.fetchall()
        self.write_to_csv(write_file_2, records)
                
    def exclude_file(self, write_file_3):
        self.cur.execute("SELECT products.description,products.amount,products.is_active FROM products WHERE is_active = 'false';")
        records = self.cur.fetchall()
        self.write_to_csv(write_file_3, records)

   
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("output_file1",help="Place output file here")
    parser.add_argument("output_file2",help="Place output file here")
    parser.add_argument("output_file3",help="Place output file here")
    args = parser.parse_args()
    db = ProductDatabase()
    db.add_column()
    db.sort_file(args.output_file1)
    db.actv_amnt_file(args.output_file2)
    db.exclude_file(args.output_file3)