import json, time, random, math
from kafka import KafkaProducer
from kafka.errors import KafkaError

REDPANDA_TOPIC = 'footfall-in'
MAIN_ZONE_SENSOR_ID = 0
SUB1_ZONE_SENSOR_ID = 1
SUB2_ZONE_SENSOR_ID = 2
SUB3_ZONE_SENSOR_ID = 3
SUB4_ZONE_SENSOR_ID = 4
TOTAL_SENSOR_COUNT = 5

total_persons_in = 0

def on_send_error(e):
  print(f"Error in publidhing footfall sensor data: {e}")

def get_sensor_reading(id):

  global total_persons_in

  if(id == MAIN_ZONE_SENSOR_ID):
    #Main zone footfall is simulated based on normal distributrion with mean of 50 and std. deviation of 25.
    mu = 50  
    sigma = 25
    total_persons_in = math.floor(random.gauss(mu, sigma))
    
    if(total_persons_in>0):
      person_count = total_persons_in
    else:
      total_persons_in = 0
      person_count = 0

  else:
    #Sub zone footfall is simulated based on assumption that half of main zone traffic can be accomodated in 
    #sub zones and a rush factor wherein customers visit a sub zone multple times.
    usual_footfall = random.randrange(0,(int(total_persons_in/2) + 1))
    rush_factor = random.randrange(1,4)
    person_count = usual_footfall * rush_factor

  print(f'Sensor id : {id} , Footfall count: {person_count}')
  return person_count

if __name__ == '__main__':

  try:
    producer = KafkaProducer(
      bootstrap_servers = "localhost:9092",
      value_serializer=lambda m: json.dumps(m).encode('ascii')
    )
    print("Simulating sensor readings for last one hour with 4 intervals of 15 mins.")
    hour_interval = ["1 - 15 mins", " 16 - 30 mins" , " 31 - 45 mins" , "46 - 60 mins"]

    for interval in range(len(hour_interval)):
      print(f"Publishing footfall sensor data for interval : {hour_interval[interval]}")

      for sensor_id in range(MAIN_ZONE_SENSOR_ID,TOTAL_SENSOR_COUNT):
        producer.send(REDPANDA_TOPIC,{ 
          "zoneCount" : get_sensor_reading(sensor_id), 
          "zoneId" : sensor_id
        }).add_errback(on_send_error)
        producer.flush()

      time.sleep(2)
        
  except KeyboardInterrupt:
    print("Publishing interrupted")

  except (KafkaError, Exception) as e:
    print(" Exception ..",e)

  finally:
    producer.close()