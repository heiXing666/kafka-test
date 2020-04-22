import config
import time

from kafka import KafkaConsumer


class ConsumeKafka:
    def __init__(self):
        self.consume = KafkaConsumer(
            config.TOPIC,
            bootstrap_servers=config.SERVER,
            group_id='heixing',
            auto_offset_reset='earliest'
        )

    def start(self):
        for msg in self.consume:
            print(msg.value.decode())
            time.sleep(0.2)

    def main(self):
        self.start()


if __name__ == '__main__':
    spider = ConsumeKafka()
    spider.main()