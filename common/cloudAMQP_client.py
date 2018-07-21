import pika
#Pika is a pure-Python implementation of the AMQP 0-9-1 protocol including RabbitMQ's extensions.
import json

class CloudAMQPClient:
    def __init__(self, cloud_amqp_url, queue_name):
        self.could_amp_url = cloud_amqp_url
        self.queue_name = queue_name
        self.params = pika.URLParameters(cloud_amqp_url)
        self.params.socket_timeout = 3
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)

#send a message

    def sendMessage(self, message):
        self.channel.basic_publish(exchange='', 
                                    routing_key=self.queue_name, 
                                    body=json.dumps(message))
        print("Sent message to %s: %s" % (self.queue_name, message))
        return 

    def getMessage(self):
        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)
        # self.checkQueueLength()

        if method_frame is not None:
            print("[0] Received message from %s: %s" % (self.queue_name, body)) 
            print(f'delivery_tag is: {method_frame.delivery_tag}')
            # self.channel.basic_ack(method_frame.delivery_tag) # **
            return json.loads(body) 
        else:
            print('No message returned')
            return None
    
    def checkQueueLength(self):
        res = self.channel.queue_declare(
                    queue="test",
                    durable=True,
                    exclusive=False,
                    auto_delete=False
                )
        print('Messages in queue %d' % res.method.message_count)        

    # sleep
    def sleep(self, seconds):
        self.connection.sleep(seconds)


