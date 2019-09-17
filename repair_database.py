import psycopg2
from datetime import date, timedelta
import json
import getopt
import os, sys, shutil
from os import path

settings_path = os.path.join(os.path.dirname(__file__), "settings.json")
with open(settings_path) as jsonData:
    settings = json.load(jsonData)
    jsonData.close()

hostname = settings['postgres']['hostname']
username = settings['postgres']['username']
password = settings['postgres']['password']
database = settings['postgres']['database']

# set default date to yesterday



def main(argv):
    # parse commandline parameters
    yesterday = date.today() - timedelta(1)
    subjectDate = yesterday.strftime('%Y-%m-%d')
    dateParts = subjectDate.split("-")
    subjectYear = dateParts[0]
    subjectMonth = dateParts[1]
    subjectDay = dateParts[2]
    rackNum = ''
    try:
        opts, args = getopt.getopt(argv, "r:d:")
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    if opts is None:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-r':
            rackNum = arg
        elif opt == '-d':
            subjectDate = arg
            dateParts = subjectDate.split("-")
            subjectYear = dateParts[0]
            subjectMonth = dateParts[1]
            subjectDay = dateParts[2]
            if len(dateParts) != 3 or len(subjectYear) != 4 or len(subjectMonth) != 2 or len(subjectDay) != 2 :
                print("Invalid date")
                usage()
                sys.exit(2)

    if rackNum == '' or subjectDate == '':
        usage()
        sys.exit(2)

    # setup DB
    date_recorded_from = subjectYear + "-" + (subjectMonth-1) + "-" + subjectDay + ''
    date_recorded_to = subjectYear + "-" + subjectMonth + "-" + subjectDay + ''
    fetch_rows = ''' SELECT url,shid from shelf_stock WHERE date_recorded>='{0}' 
    and date_recorded<'{1}' and racknum='{2}' '''.format(date_recorded_from, date_recorded_to, rackNum)
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cursor = conn.cursor()
    cursor.execute(fetch_rows)
    print(fetch_rows)
    shelf_stock_rows = []
    if cursor.rowcount > 0:
        shelf_stock_rows = cursor.fetchall()

    for row in shelf_stock_rows:
        found_url = row[0]
        found_id = row[1]
        if 'smart-rack-bucket.s3' in found_url:
            changed_url = found_url.replace('smart-rack-bucket.s3', 'smart-rack-bucket-store.s3')
            print(changed_url)
            #update_record = '''UPDATE shelf_stock SET url={0} WHERE shid={1}'''.format(changed_url, found_id)
            #cursor.execute(update_record)
            #conn.commit()

def usage():
    print("python processrackimages.py -r 000000 -d YYYY-MM-DD")
    print("  -r 000000 : unique rack ID number")
    print("  -d YYYY-MM-DD : UTC date ex: 2016-11-28")

# call main function
if __name__ == "__main__":
    main(sys.argv[1:])