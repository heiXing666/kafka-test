import config
import json
import time
import datetime


from kafka import KafkaProducer


class TestKafka:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=config.SERVER,
                                      value_serializer=lambda m: json.dumps(m).encode())
        print(self.producer)

    def on_send_success(self, record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_send_error(self, excp):
        print('I am an errback')
        print(excp)

    def start(self):
        for i in range(1, 1000):
            data = {'num': i, 'ts': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            self.producer.send(config.TOPIC, data).add_callback(self.on_send_success).add_errback(self.on_send_error)
            # time.sleep(0.1)

    def main(self):
        self.start()


if __name__ == '__main__':
    spider = TestKafka()
    spider.main()