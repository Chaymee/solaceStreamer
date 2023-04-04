# producer.py

from email.message import Message
import time
import sys
import cv2
from solace.messaging.config import _sol_constants
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.utils.manageable import Metric


# Broker Config. Note: Could pass other properties Look into
broker_props = {
    "solace.messaging.transport.host": '',
    "solace.messaging.service.vpn-name": '', 
    "solace.messaging.authentication.scheme.basic.username": '',
    "solace.messaging.authentication.scheme.basic.password": ''
    }
TOPIC = ''
INPUTFILE = ''

def video_emitter():
    # Open the video file
    print('Reading video file: ' + INPUTFILE)
    try:
        video = cv2.VideoCapture(INPUTFILE)
    except:
        exit(0)

    print(' emitting.....')
    messageService = MessagingService.builder().from_properties(broker_props).with_message_compression(compression_factor=9).build()
    
    messageService.connect()
    destination_name = Topic.of(TOPIC)
    
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        #downscale video
        scale_percent = 100
        width = int(image.shape[1] * scale_percent / 100)
        height = int(image.shape[0] * scale_percent / 100)
        dim = (width, height)
        output = cv2.resize(image, dim, interpolation= cv2.INTER_AREA)
        # Check if the file has been read to the end
        if not success:
            break
        # Convert image to png
        ret, jpeg = cv2.imencode('.png', output)
        #encode_parm = [int(cv2.IMWRITE_JPEG_QUALITY), 10]
        #ret, jpeg = cv2.imencode('.png', image, encode_parm)
        # Convert image to bytes and send over kafka

        direct_message_publish(messageService, destination_name, bytearray(jpeg))

        # Sleep to curtail CPU usage
        #time.sleep(0.2)
    # Clear the capture
    video.release()
    print('done emitting')


def direct_message_publish(messaging_service: MessagingService, destination, message):

    try:
        direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
        pub_start = direct_publish_service.start_async()
        pub_start.result()
        direct_publish_service.publish(destination=destination,message=message)
    finally:
        direct_publish_service.terminate()


def readParms():
    global broker_props
    global TOPIC
    global INPUTFILE
    print('args: ' + str(sys.argv) + str(len(sys.argv)))
    if len(sys.argv) != 7:
        print('**USAGE** host vpnname userID password topic infile') 
        exit()
    print('Command line arguments: ' + str(sys.argv))
    broker_props["solace.messaging.transport.host"] = str(sys.argv[1])
    broker_props["solace.messaging.service.vpn-name"] = str(sys.argv[2])
    broker_props["solace.messaging.authentication.scheme.basic.username"] = str(sys.argv[3])
    broker_props["solace.messaging.authentication.scheme.basic.password"] = str(sys.argv[4])
    TOPIC = str(sys.argv[5])
    INPUTFILE = str(sys.argv[6])



if __name__ == '__main__':
    readParms()
    video_emitter()