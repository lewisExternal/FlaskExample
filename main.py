import sqlite3
from flask import Flask
from flask import request, jsonify
import pandas as np 
import db_lib

app = Flask(__name__)

def main():
  
    # define database name 
    database = r"pythonsqlite.db"
    
    @app.route('/api/v1/report', methods=['GET'])
    def return_report():

        if 'date' in request.args:
            date = str(request.args['date'])
        else:
            return "Error: No date field provided. Please specify a date."
        
        # get connection to db 
        conn = db_lib.create_connection(database)
        print("Connection to the DB established.")

        # query for the report 
        if conn is not None:
            result = db_lib.sql_report(conn,date)
        else: 
            return "Error: DB call failed."

        promotions = {
                        "1": result[8],
                        "2": result[9],
                        "3": result[10],
                        "4": result[11],
                        "5": result[12]
                    }

        commissions = {
                        "promotions": promotions,
                        "total": result[6],
                        "order_average": result[7]
                    }

        resultDict = {
                        "items": result[1],
                        "customers": result[2],
                        "total_discount_amount": result[3],
                        "order_total_avg": result[5],
                        "discount_rate_avg":result[4],
                        "commissions": commissions
                    }

        return jsonify(resultDict)
   
    # run the app 
    app.run()

if __name__ == '__main__':
    main()