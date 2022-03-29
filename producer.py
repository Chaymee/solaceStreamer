# producer.py

from email.message import Message
import time
import sys
import cv2
from solace.messaging.config import _sol_constants
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.utils.converter import ObjectToBytes
from solace.messaging.utils.manageable import Metric


# Broker Config. Note: Could pass other properties Look into
broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "tcp://localhost:55555,tcp://localhost:55554",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "default"
    }
TOPIC = ''
INPUTFILE = ''

def video_emitter(video):
    # Open the video file
    video = cv2.VideoCapture(video)
    print(' emitting.....')
    messageService = MessagingService.builder().from_properties(broker_props)\
                        .build()
    
    messageService.connect()
    
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # Check if the file has been read to the end
        if not success:
            break
        # Convert image to png
        ret, jpeg = cv2.imencode('.png', image)
        # Convert image to bytes and send over kafka
        try:
            messagingService.connect()
            destination = Topic.of('solace/test/topic')
        finally: 
            print('didnt send')
        direct_message_publish(messagingService, destination, bytearray(jpeg))
        # Sleep to curtail CPU usage
        time.sleep(0.2)
    # Clear the capture
    video.release()
    print('done emitting')


def direct_message_publish(messaging_service: MessagingService, destination, message):

    try:
        direct_publish_service = messaging_service.create_direct_message_publisher_builder().\
            on_back_pressure_reject(buffer_capacity=0).build()
        pub_start = direct_publish_service.start_async()
        pub_start.result()
        direct_publish_service.publish(destination=destination,message=message)
    finally:
        direct_publish_service.terminate()


def readParms():
    if sys.argv.count != 7:
        print('**USAGE** host vpnname userID password topic infile') 
        exit()
    print('Command line arguments: ' + str(sys.argv))
    broker_props["solace.messaging.transport.host"] = str(sys.argv[1])
    broker_props["solace.messaging.service.vpn-name"] = str(sys.argv[2])
    broker_props["solace.messaging.authentication.scheme.basic.username"] = str(sys.argv[3])
    broker_props["solace.messaging.authentication.scheme.basic.password"] = str(sys.argv[4])
    TOIPC = str(sys.argv[5])
    INPUTFILE = str(sys.argv[6])



if __name__ == '__main__':
    video_emitter('video.mp4')