"""
https://github.com/confluentinc/confluent-kafka-python/issues/418
https://github.com/sucitw/benchmark-python-client-for-kafka
"""
from confluent_kafka import Consumer, KafkaError, serialization
import time, sys
import datetime
import psycopg2
from mobilitydb.psycopg import register

VEHICLENUMBER = 1000

def kafka_tuple_to_sql(msg):
    return tuple(msg.value().decode('utf8').split(','))
    
def consumeToMobilityDB():
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

    connection = psycopg2.connect(user="docker",
                                  password="docker",
                                  host="127.0.0.1",
                                  port="25432",
                                  database="benchInsert")                     
    connection.autocommit = True
    register(connection)
    
    c = Consumer(settings)
    c.subscribe(['gpsPosition'])
    consume_time = None
    msgcount = 0
    cursor = connection.cursor()

    try:
        while True:
            creation_time = datetime.datetime.now()

            msgList = c.consume(num_messages=VEHICLENUMBER,timeout=1)
            avg_insert_time = 0

            if len(msgList):
                
                for x in msgList:
                    onePosition = kafka_tuple_to_sql(x)

                    args_str_positions = "tgeompointinst(ST_Transform(ST_SetSRID( ST_MakePoint( {0}, {1}), 4326), 25832), '{2}')".format(onePosition[0],onePosition[1],onePosition[2])
                    
                    oneinserttimestart = datetime.datetime.now()
                    cursor.execute("""INSERT INTO shipspositions 
                                        VALUES("""+onePosition[3]+""", tgeompointseq(array["""+args_str_positions+"""
	                                        ]		
	                                        ))

                                        ON CONFLICT (shipid) DO UPDATE 
                                          SET
                                        trip = appendInstant(shipspositions.trip, tgeompoint("""+args_str_positions+"""		
	                                        ));""") 

                    connection.commit()
                    oneinserttimeend = datetime.datetime.now()
                    
                    dt = oneinserttimeend - oneinserttimestart
                    if avg_insert_time == 0:
                        avg_insert_time = dt
                    else:
                        avg_insert_time += dt
                        avg_insert_time = avg_insert_time / 2
                    
                insert_time = datetime.datetime.now()
                delta = insert_time-creation_time
                
                print(str(len(msgList))+" Positions in "+str(delta))
                print(" Avg SQL insert time : "+str(avg_insert_time))
                
    finally:
        c.close()
        cursor.close()
        connection.close()

def main(argv):
    consumeToMobilityDB()
    
if __name__ == "__main__":
    main(sys.argv)
    
