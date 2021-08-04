import traceback
import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def create_table(tableName, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute('DROP TABLE IF EXISTS %s' % tableName)
            cur.execute('CREATE TABLE IF NOT EXISTS %s (userid int, movieid int, rating float)' % tableName)
            #print('Table [{0}] created'.format(tableName))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    try:
        with openconnection.cursor() as cur:
            create_table(ratingstablename, openconnection)
            with open(ratingsfilepath) as f:
                for line in f:
                    words = line.split('::')
                    cur.execute('insert into %s (userid, movieid, rating) values(%s,%s,%s)' % (ratingstablename, words[0], words[1], words[2]))
        print('Ratings loaded')
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def delete_all_partitions(prefix, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute("select table_name from information_schema.tables where table_name ilike '{0}%'".format(prefix))
            partition_tables = []
            for row in cur:
                partition_tables.append(row[0])
            for tableName in partition_tables:
                cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(tableName))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def update_round_robin_metadata(openconnection, round_robin_total_partitions=-1, round_robin_last_inserted=-1):
    try:
        with openconnection.cursor() as cur:
            cur.execute(
                'CREATE TABLE IF NOT EXISTS ROUND_ROBIN_METADATA (round_robin_total_partitions int, round_robin_last_inserted int)')
            cur.execute('select count(*) from ROUND_ROBIN_METADATA')
            count = cur.fetchone()[0]
            if (count == 0):
                cur.execute(
                    'INSERT INTO ROUND_ROBIN_METADATA (round_robin_total_partitions, round_robin_last_inserted) VALUES(0,-1)')
                print('Inserting default values into ROUND_ROBIN_METADATA table')

            if (round_robin_total_partitions >= 0):
                cur.execute('UPDATE ROUND_ROBIN_METADATA SET round_robin_total_partitions = {0}'.format(
                    round_robin_total_partitions))
                print('Updated round_robin_total_partitions to {0}'.format(round_robin_total_partitions))

            if (round_robin_last_inserted >= 0):
                cur.execute(
                    'UPDATE ROUND_ROBIN_METADATA SET round_robin_last_inserted = {0}'.format(round_robin_last_inserted))
                print('Updated round_robin_last_inserted to {0}'.format(round_robin_last_inserted))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def update_range_metadata(openconnection, range_total_partitions = -1, min_value = -1.0, max_value = -1.0, interval = -1.0):
    try:
        with openconnection.cursor() as cur:
            cur.execute('CREATE TABLE IF NOT EXISTS RANGE_METADATA (range_total_partitions int, min_value float, max_value float, interval float)')
            cur.execute('select count(*) from RANGE_METADATA')
            count = cur.fetchone()[0]
            if(count == 0):
                cur.execute('INSERT INTO RANGE_METADATA (range_total_partitions, min_value, max_value, interval) VALUES(0,0.0,5.0,0)')
                print('Inserting default values into RANGE_METADATA table')

            if(range_total_partitions >= 0):
                cur.execute('UPDATE RANGE_METADATA SET range_total_partitions = {0}'.format(range_total_partitions))
                print('Updated range_total_partitions to {0}'.format(range_total_partitions))
            if(min_value >= 0):
                cur.execute('UPDATE RANGE_METADATA SET min_value = {0}'.format(min_value))
                print('Updated min_value to {0}'.format(min_value))
            if(max_value >= 0):
                cur.execute('UPDATE RANGE_METADATA SET max_value = {0}'.format(max_value))
                print('Updated max_value to {0}'.format(max_value))
            if(interval >= 0):
                cur.execute('UPDATE RANGE_METADATA SET interval = {0}'.format(interval))
                print('Updated interval to {0}'.format(interval))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
            print('numberofpartitions not an instance of int or <= 0')
            return

        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % ratingstablename)
            count = cur.fetchone()[0]

            if (count == 0):
                print('%s table does not exist' % ratingstablename)
                return

            cur.execute("select * from %s" % ratingstablename)
            tableContents = cur.fetchall()

            delete_all_partitions(RANGE_TABLE_PREFIX, openconnection)
            for i in range(0, numberofpartitions):
                create_table('{0}{1}'.format(RANGE_TABLE_PREFIX, i), openconnection)

            minVal = 0.0
            maxVal = 5.0
            increment = (maxVal - minVal) / numberofpartitions
            rowsToInsert = []

            for i in range(0, numberofpartitions):
                rowsToInsert = []
                upperBound = (minVal + increment) if (i < numberofpartitions - 1) else maxVal
                if (i == 0):
                    for row in tableContents:
                        if (minVal <= row[2] <= upperBound):
                            rowsToInsert.append(row)
                else:
                    for row in tableContents:
                        if (minVal < row[2] <= upperBound):
                            rowsToInsert.append(row)
                query = 'INSERT INTO {0}{1} (userid, movieid, rating) VALUES(%s,%s,%s)'.format(RANGE_TABLE_PREFIX, i)
                cur.executemany(query, rowsToInsert)
                minVal = upperBound

            update_range_metadata(openconnection, range_total_partitions=numberofpartitions,
                                  min_value=0.0, max_value=5.0, interval=increment)

        print('Range Partitioning done')
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
            print('numberofpartitions not an instance of int or <= 0')
            return
        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % ratingstablename)
            count = cur.fetchone()[0]
            if (count == 0):
                print('%s table does not exist' % ratingstablename)
                return

            cur.execute("select * from %s" % ratingstablename)
            tableContents = cur.fetchall()

            delete_all_partitions(RROBIN_TABLE_PREFIX, openconnection)
            for i in range(0, numberofpartitions):
                create_table('{0}{1}'.format(RROBIN_TABLE_PREFIX, i), openconnection)

            lastInserted = -1
            for row in tableContents:
                lastInserted = (lastInserted + 1) % numberofpartitions
                partitionTableName = RROBIN_TABLE_PREFIX + str(lastInserted)
                cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES({1},{2},{3})'.format(
                    partitionTableName, row[0], row[1], row[2]))

            update_round_robin_metadata(openconnection, round_robin_total_partitions=numberofpartitions,
                                        round_robin_last_inserted=lastInserted)
        print('Round Robin Partitioning done')
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % ratingstablename)
            count = cur.fetchone()[0]

            if (count == 0):
                print('%s table does not exist' % ratingstablename)
                return

            cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES({1},{2},{3})'.format(ratingstablename,
                                                                                               userid, itemid, rating))
            print('Insertion done in table [{0}]'.format(ratingstablename))

            cur.execute(
                'select round_robin_last_inserted, round_robin_total_partitions from ROUND_ROBIN_METADATA LIMIT 1')
            lastInserted, totalPartitions = cur.fetchone()

            if (totalPartitions <= 0):
                print('Total partitions <= 0')
                return

            nextPartition = (lastInserted + 1) % totalPartitions
            partitionTableName = RROBIN_TABLE_PREFIX + str(nextPartition)
            cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES({1},{2},{3})'.format(partitionTableName,
                                                                                               userid, itemid, rating))
            update_round_robin_metadata(openconnection, round_robin_last_inserted=nextPartition)
        print('Round Robin insertion done in table [{0}]'.format(partitionTableName))
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        with openconnection.cursor() as cur:
            cur.execute("select count(*) from information_schema.tables where table_name ilike '%s'" % ratingstablename)
            count = cur.fetchone()[0]

            if (count == 0):
                print('%s table does not exist' % ratingstablename)
                return

            cur.execute('INSERT INTO {0} (userid, movieid, rating) VALUES({1},{2},{3})'.format(ratingstablename,
                                                                                               userid, itemid, rating))
            print('Insertion done in table [{0}]'.format(ratingstablename))

            cur.execute('select range_total_partitions, min_value, max_value from RANGE_METADATA LIMIT 1')
            totalPartitions, minVal, maxVal = cur.fetchone()

            if (totalPartitions <= 0):
                print('Total partitions <= 0')
                return

            interval = (maxVal - minVal) / totalPartitions

            for i in range(0, totalPartitions):
                upperBound = (minVal + interval) if (i < totalPartitions - 1) else maxVal

                insert = False
                if (i == 0):
                    if (minVal <= rating <= upperBound):
                        insert = True
                else:
                    if (minVal < rating <= upperBound):
                        insert = True

                if (insert):
                    tableName = RANGE_TABLE_PREFIX + str(i)
                    cur.execute(
                        'INSERT INTO {0} (userid, movieid, rating) VALUES({1},{2},{3})'.format(tableName, userid,
                                                                                               itemid, rating))
                    print('Range insertion done in table [{0}]'.format(tableName))

                minVal = upperBound
    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    try:
        with openconnection.cursor() as cur:
            cur.execute('select range_total_partitions, min_value, max_value from RANGE_METADATA LIMIT 1')
            totalRangePartitions, minVal, maxVal = cur.fetchone()
            cur.execute(
                'select round_robin_last_inserted, round_robin_total_partitions from ROUND_ROBIN_METADATA LIMIT 1')
            roundRobinlastInserted, roundRobinTotalPartitions = cur.fetchone()

            result = []
            # taking from range partitions
            interval = (maxVal - minVal) / totalRangePartitions
            selectMode = False
            for i in range(0, totalRangePartitions):
                upperBound = (minVal + interval) if (i < totalRangePartitions - 1) else maxVal
                if (not selectMode):
                    if (i == 0 and minVal <= ratingMinValue <= upperBound) or (minVal < ratingMinValue <= upperBound):
                        selectMode = True
                if (selectMode):
                    tableName = RANGE_TABLE_PREFIX + str(i)
                    cur.execute("select '{0}', * from {0} where rating >= {1} and rating <= {2} order by rating".format(
                        tableName,
                        ratingMinValue,
                        ratingMaxValue))
                    result += cur.fetchall()

                if (i == 0 and minVal <= ratingMaxValue <= upperBound) or (minVal < ratingMaxValue <= upperBound):
                    # if max value also falls in this range, break
                    break
                minVal = upperBound

            # taking from round robin partitions
            for i in range(0, roundRobinTotalPartitions):
                tableName = RROBIN_TABLE_PREFIX + str(i)
                cur.execute("select '{0}', * from {0} where rating >= {1} and rating <= {2}".format(tableName,
                                                                                                    ratingMinValue,
                                                                                                    ratingMaxValue))
                result += cur.fetchall()

            result = list(map(lambda item: ','.join(str(val) for val in item), result))  # here item is a tuple
            fileContents = '\n'.join(result)

            with open(outputPath, 'w+') as f:
                f.writelines(fileContents)

    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def pointQuery(ratingValue, openconnection, outputPath):
    try:
        with openconnection.cursor() as cur:
            cur.execute('select range_total_partitions, min_value, max_value from RANGE_METADATA LIMIT 1')
            totalRangePartitions, minVal, maxVal = cur.fetchone()
            cur.execute(
                'select round_robin_last_inserted, round_robin_total_partitions from ROUND_ROBIN_METADATA LIMIT 1')
            roundRobinlastInserted, roundRobinTotalPartitions = cur.fetchone()

            result = []
            # taking from range partitions
            interval = (maxVal - minVal) / totalRangePartitions
            selectMode = False
            for i in range(0, totalRangePartitions):
                upperBound = (minVal + interval) if (i < totalRangePartitions - 1) else maxVal
                if (i == 0 and minVal <= ratingValue <= upperBound) or (minVal < ratingValue <= upperBound):
                    tableName = RANGE_TABLE_PREFIX + str(i)
                    cur.execute(
                        "select '{0}', * from {0} where rating = {1} order by rating".format(tableName, ratingValue))
                    result += cur.fetchall()
                    break
                minVal = upperBound

            # taking from round robin partitions
            for i in range(0, roundRobinTotalPartitions):
                tableName = RROBIN_TABLE_PREFIX + str(i)
                cur.execute("select '{0}', * from {0} where rating = {1}".format(tableName, ratingValue))
                result += cur.fetchall()

            result = list(map(lambda item: ','.join(str(val) for val in item), result))  # here item is a tuple
            fileContents = '\n'.join(result)

            with open(outputPath, 'w+') as f:
                f.writelines(fileContents)

    except Exception as e:
        print('Exception occurred: {0}'.format(e))
        traceback.print_exc()

def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
