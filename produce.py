import config
import json
import time
import datetime

from kafka import KafkaProducer


class TestKafka:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=config.SERVER,
                                      value_serializer=lambda m: json.dumps(m).encode())

    def start(self):
        for i in range(1, 1000):
            data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            print(data)
            self.producer.send(config.TOPIC, data)
            time.sleep(0.1)

    def main(self):
        self.start()


if __name__ == '__main__':
    spider = TestKafka()
    spider.main()