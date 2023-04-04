# consumer.py
from flask import Flask, Response
import sys

from solace.messaging.resources.queue import Queue
from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.utils.converter import BytesToObject
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver

# Broker Config. Note: Could pass other properties Look into
broker_props = {
    "solace.messaging.transport.host": '',
    "solace.messaging.service.vpn-name": '', 
    "solace.messaging.authentication.scheme.basic.username": '',
    "solace.messaging.authentication.scheme.basic.password": ''
    }



# Continuously listen to the connection and print a recieved message
app = Flask(__name__)

@app.route('/')
def index():
    # return a multipart response
    return Response(solaceStream(), 
                mimetype='multipart/x-mixed-replace; boundary=frame')

def solaceStream():
    
    while True:
        
        received = receiver.receive_message()
        message_payload = received.get_payload_as_bytes()
        receiver.ack(received)
        print('recieved message')
        yield (b'--frame\r\n'
                b'Content-Type: image/png\r\n\r\n' + message_payload + b'\r\n\r\n')

def readParms():
    global broker_props
    global TOPIC
    global INPUTFILE
    print('args: ' + str(sys.argv) + str(len(sys.argv)))
    if len(sys.argv) != 6:
        print('**USAGE** host vpnname userID password topic infile') 
        exit()
    print('Command line arguments: ' + str(sys.argv))
    broker_props["solace.messaging.transport.host"] = str(sys.argv[1])
    broker_props["solace.messaging.service.vpn-name"] = str(sys.argv[2])
    broker_props["solace.messaging.authentication.scheme.basic.username"] = str(sys.argv[3])
    broker_props["solace.messaging.authentication.scheme.basic.password"] = str(sys.argv[4])
    TOPIC = str(sys.argv[5])

                
if __name__ == '__main__':
    readParms()
    # Connect to solace server and pass the topic we want to consume
    service = MessagingService.builder().from_properties(broker_props).build()
    service.connect()
    topics = [TopicSubscription.of(TOPIC)]

    # try with queues
    queue = Queue.durable_exclusive_queue(TOPIC)
    receiver = service.create_persistent_message_receiver_builder().build(queue)

    #receiver: DirectMessageReceiver = service.create_direct_message_receiver_builder().with_subscriptions(topics).build()
    receiver.start()
    app.run()