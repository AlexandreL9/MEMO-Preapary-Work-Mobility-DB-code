"""
https://github.com/confluentinc/confluent-kafka-python/issues/418
https://github.com/sucitw/benchmark-python-client-for-kafka
"""
from confluent_kafka import Consumer, KafkaError, serialization
import time, sys
import datetime
import psycopg2
from mobilitydb.psycopg import register
import _thread
from multiprocessing import Process, Manager




BUFFERSIZE = 50
VEHICLESNUMBER = 1000
print("BUFFERSIZE : ")
print(BUFFERSIZE)
print("VEHICLESNUMBER : ")
print(VEHICLESNUMBER)

def kafka_tuple_to_sql(msg):
    return tuple(msg.value().decode('utf8').split(','))
    
def consumeToMobilityDB(BUFFER):
    """
    consume from kafka and write to postgresql
    """    
    # Consumer setup
    settings = {
        'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup',
        'client.id': 'client-1',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'broker.version.fallback': '0.9.0.0',
        'api.version.request': False,
        'default.topic.config': {'auto.offset.reset': 'smallest'}}
    
    c = Consumer(settings)
    c.subscribe(['gpsPosition'])
    # uncomment this code to purge kafka Buffer
    """
    while True:
        msgList = c.consume(num_messages=VEHICLESNUMBER,timeout=1)
    """
    try:
        while True:     
            msgList = c.consume(num_messages=VEHICLESNUMBER,timeout=1)
            if len(msgList):
                while len(msgList) > 0:
                    x = msgList[-1]
                    onePosition = kafka_tuple_to_sql(x)
                    if len(BUFFER[int(onePosition[3])]) <= BUFFERSIZE:
                        BUFFER[int(onePosition[3])].append(onePosition)
                        msgList.pop()
                        
                        
    finally:
        c.close()
 
def insertDB_thread(BUFFER):
    connection = psycopg2.connect(user="docker",
                                  password="docker",
                                  host="127.0.0.1",
                                  port="25432",
                                  database="benchInsert")                     
    connection.autocommit = True
    register(connection)
    cursor = connection.cursor()
    
    
    try:
        while True:
            creation_time = datetime.datetime.now()
            counter = 0
            avg_insert_time_global = 0
            avg_insert_time = 0
            v_vounter = 0
            for v in BUFFER.keys(): 
                print(len(BUFFER[v]))
                if len(BUFFER[v]) >= BUFFERSIZE:
                    v_vounter += 1
                    args_str_positions = ""
                    insert_time_one_buffer_start = datetime.datetime.now()
                    for i in range(0,len(BUFFER[v])):

                        if not args_str_positions == "":
                            args_str_positions += ','

                        onePosition = BUFFER[v][i] 
                        args_str_positions += "tgeompointinst(ST_Transform(ST_SetSRID( ST_MakePoint( {0}, {1}), 4326), 25832), '{2}')".format(onePosition[0],onePosition[1],onePosition[2])
                    oneinserttimestart = datetime.datetime.now()
                    cursor.execute("""INSERT INTO shipspositions 
                                    VALUES("""+onePosition[3]+""", tgeompointseq(array["""+args_str_positions+"""
                                        ]		
                                        ))

                                    ON CONFLICT (shipid) DO UPDATE 
                                      SET
                                    trip = merge(shipspositions.trip, tgeompointseq(array["""+args_str_positions+"""		
                                        ]));""") 
                    oneinserttimeend = datetime.datetime.now()

                    counter += len(BUFFER[v])
                    connection.commit()
                    BUFFER[v][:] = []
                    

                    insert_time_one_buffer_end = datetime.datetime.now()
                    insert_time_one_buffer_delta = insert_time_one_buffer_end - insert_time_one_buffer_start
                    if avg_insert_time_global == 0:
                        avg_insert_time_global = insert_time_one_buffer_delta
                    else:
                        avg_insert_time_global += insert_time_one_buffer_delta
                        avg_insert_time_global = avg_insert_time_global / 2
                       
                    dt = oneinserttimeend - oneinserttimestart
                    if avg_insert_time == 0:
                        avg_insert_time = dt
                    else:
                        avg_insert_time += dt
                        avg_insert_time = avg_insert_time / 2

                    
            insert_time = datetime.datetime.now()
            delta = insert_time-creation_time
            if counter > 0:
                print(" Inserted and merged "+str(counter)+" positions from "+str(v_vounter)+" vehicles  in "+str(delta))
                print(" Avg insert time one vehicle BUFFER of size "+str(BUFFERSIZE)+" : "+str(avg_insert_time_global))
                print(" Avg SQL insert time one vehicle BUFFER : "+str(avg_insert_time))

    finally:
        cursor.close()
        connection.close()

def main(argv):
    manager = Manager()

    BUFFER = manager.dict()
    for i in range(1,VEHICLESNUMBER+2):
        BUFFER[i] = manager.list()
    
    p1 = Process(target=insertDB_thread, args=(BUFFER,))
    p2 = Process(target=consumeToMobilityDB, args=(BUFFER,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()


if __name__ == "__main__":
    main(sys.argv)
    
    
