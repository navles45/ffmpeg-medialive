import boto3
from botocore.config import Config
import datetime
import subprocess
import threading
import keyboard

my_config = Config(
    region_name = REGION_NAME,
    signature_version = 'v4',
    retries = {
        'max_attempts' : 10,
        'mode' : 'standard'
    }
)

access_key_id = ACCESS_KEY_ID
access_secret_key = ACCESS_SECRET_KEY

client = boto3.client('medialive', config=my_config, aws_access_key_id=access_key_id, aws_secret_access_key=access_secret_key)
s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=access_secret_key)

# List S3 Object
bucket_object = s3_client.list_objects(Bucket=BUCKET_NAME)

# Get Object Name
object_properties = bucket_object.get('Contents')
object_name = object_properties[0]['Key']

# Generate presigned url
object_url = s3_client.generate_presigned_url(
    ClientMethod='get_object',
    Params={
        'Bucket':BUCKET_NAME,
        'Key':object_name,
        },
    ExpiresIn=3800,
    )

def create_channel_input():
    global input
    input = client.create_input(
        Destinations=[
            {
                'StreamName': stream_name,
            },
        ],
        InputSecurityGroups=[
            SECURITY_GROUP,
        ],
        Name = instance_name,
        Type='RTMP_PUSH',

    )
    return input


def channel_creation():
    #Create Input
    global input_id
    input_id = input['Input']['Id']
    print(input_id)
    input_name = input['Input']['Name']
    print(input_name)
    global input_url
    input_url = input['Input']['Destinations'][0]['Url']
    print(input_url)

    response = client.create_channel(ChannelClass='SINGLE_PIPELINE',
        Destinations=[
        {
            'Id': id_name,
            'MediaPackageSettings': [
                {
                    'ChannelId': channel_id,
                },
            ],
        },
    ],
        EncoderSettings = {
            'AudioDescriptions':[
            {
                'AudioSelectorName':'Audio128aac',
                'CodecSettings':{
                    'AacSettings':{
                        'Bitrate':128000,
                        'InputType':'NORMAL',
                        'Profile':'LC',
                        'RateControlMode':'CBR',
                        'RawFormat':'NONE',
                        'SampleRate':48000,
                        'Spec':'MPEG4'
                    },
                },
                'Name': 'audio1aac128',
            },
            {
                'AudioSelectorName':'Audio128aac',
                'CodecSettings':{
                    'AacSettings':{
                        'Bitrate':128000,
                        'InputType':'NORMAL',
                        'Profile':'LC',
                        'RateControlMode':'CBR',
                        'RawFormat':'NONE',
                        'SampleRate':48000,
                        'Spec':'MPEG4'
                    },
                },
                'Name': 'audio2aac128',
            },
            {
                'AudioSelectorName':'Audio128aac',
                'CodecSettings':{
                    'AacSettings':{
                        'Bitrate':64000,
                        'InputType':'NORMAL',
                        'Profile':'LC',
                        'RateControlMode':'CBR',
                        'RawFormat':'NONE',
                        'SampleRate':48000,
                        'Spec':'MPEG4'
                    },
                },
                'Name': 'audio3aac128',
            },
            {
                'AudioSelectorName':'Audio64aac',
                'CodecSettings':{
                    'AacSettings':{
                        'Bitrate':128000,
                        'InputType':'NORMAL',
                        'Profile':'LC',
                        'RateControlMode':'CBR',
                        'RawFormat':'NONE',
                        'SampleRate':48000,
                        'Spec':'MPEG4'
                    },
                },
                'Name': 'audio4aac64',
            },
            ],
            'OutputGroups':[
            {
                'Name': id_name,
                'OutputGroupSettings':{
                    'MediaPackageGroupSettings':{
                        'Destination': {
                            'DestinationRefId':ref_id,
                            },
                    },

                },
                'Outputs':[
                {
                    'AudioDescriptionNames': [
                        'audio1aac128',
                    ],
                    'OutputName': '1280_720_1',
                    'OutputSettings' : {
                            'MediaPackageOutputSettings':{},
                    },
                    'VideoDescriptionName':'video_1280_720_1'
                },
                {
                    'AudioDescriptionNames': [
                        'audio2aac128',
                    ],
                    'OutputName': '1280_720_2',
                    'OutputSettings' : {
                        'MediaPackageOutputSettings':{},
                    },
                    'VideoDescriptionName':'video_1280_720_2'
                },
                {
                    'AudioDescriptionNames': [
                        'audio3aac128',
                    ],
                    'OutputName': '1920_1080',
                    'OutputSettings' : {
                        'MediaPackageOutputSettings':{},
                    },
                    'VideoDescriptionName':'video_1920_1080'
                },
                {
                    'AudioDescriptionNames': [
                        'audio4aac64',
                    ],
                    'OutputName': '640_360',
                    'OutputSettings' : {
                        'MediaPackageOutputSettings':{},
                    },
                    'VideoDescriptionName':'video_640_360'
                },
                ],
            },
        ],
        'TimecodeConfig': {
            'Source':'EMBEDDED',
            'SyncThreshold':200
        },
        'VideoDescriptions':[
            {
                'CodecSettings':{
                    'H264Settings': {
                        'Bitrate': 3000000,
                        'MaxBitrate':3500000,
                        'FramerateControl': 'SPECIFIED',
                        'FramerateDenominator': 1001,
                        'FramerateNumerator': 30000,
                        'ScanType': 'PROGRESSIVE',
                        'ParControl': 'SPECIFIED',
                        'GopSize': 60,
                        'GopSizeUnits': 'FRAMES',
                        'RateControlMode': 'CBR',

                    },
                },
                'Height':720,
                'Name': 'video_1280_720_1',
                'RespondToAfd':'NONE',
                'ScalingBehavior':'DEFAULT',
                'Width':1280
            },
            {
                'CodecSettings':{
                    'H264Settings': {
                        'Bitrate': 4500000,
                        'MaxBitrate':5000000,
                        'FramerateDenominator': 1001,
                        'FramerateNumerator': 30000,
                        'ScanType': 'PROGRESSIVE',
                        'ParControl': 'SPECIFIED',
                        'GopSize': 60,
                        'GopSizeUnits': 'FRAMES',
                        'RateControlMode': 'CBR',
                    },
                },
                'Height':720,
                'Name': 'video_1280_720_2',
                'RespondToAfd':'NONE',
                'ScalingBehavior':'DEFAULT',
                'Width':1280
            },
            {
                'CodecSettings':{
                    'H264Settings': {
                        'Bitrate': 6000000,
                        'MaxBitrate':7000000,
                        'FramerateDenominator': 1001,
                        'FramerateNumerator': 30000,
                        'ScanType': 'PROGRESSIVE',
                        'ParControl': 'SPECIFIED',
                        'GopSize': 60,
                        'GopSizeUnits': 'FRAMES',
                        'RateControlMode': 'CBR',
                    },
                },
                'Height':1080,
                'Name': 'video_1920_1080',
                'RespondToAfd':'NONE',
                'ScalingBehavior':'DEFAULT',
                'Width':1920
            },
            {
                'CodecSettings':{
                    'H264Settings': {
                        'Bitrate': 365000,
                        'MaxBitrate':380000,
                        'FramerateDenominator': 1001,
                        'FramerateNumerator': 30000,
                        'ScanType': 'PROGRESSIVE',
                        'ParControl': 'SPECIFIED',
                        'GopSize': 60,
                        'GopSizeUnits': 'FRAMES',
                        'RateControlMode': 'CBR',
                    },
                },
                'Height':360,
                'Name': 'video_640_360',
                'RespondToAfd':'NONE',
                'ScalingBehavior':'DEFAULT',
                'Width':640
            },
        ],
    },

        InputAttachments=[{

            'InputAttachmentName': input_name,
            'InputId': input_id,
        },
        ],

        InputSpecification={
            'Codec':'AVC',
            'MaximumBitrate':'MAX_10_MBPS',
            'Resolution':'HD'
        },
        Name = webinar_name,
        RoleArn = role_arn
        ),


    #Print current time
    current_time = datetime.datetime.now()
    time = current_time.strftime('%c')
    print('Channel Created'  + time )

    #Getting the Channel ID
    global Id_of_channel
    Id_of_channel = response[0]['Channel']['Id']
    print('Channel Id :' +  Id_of_channel)

    #Waiter Object
    waiter_creation = client.get_waiter('channel_created')

    waiter_creation.wait(
        ChannelId=Id_of_channel,
        WaiterConfig={
            'Delay':5,
            'MaxAttempts':6,
        }
    )

    print('Starting Channel...')
    # Starting the Channel
    response = client.start_channel(
        ChannelId=Id_of_channel
    )

    waiter_running = client.get_waiter('channel_running')

    waiter_running.wait(
        ChannelId=Id_of_channel,
        WaiterConfig={
            'Delay':15,
            'MaxAttempts':6,
        }
    )

def rtmp_stream():
    #Subprocess call FFMPEG stream RTMP
    call_rtmp = subprocess.run(['ffmpeg','-re', '-i', object_url, '-ar', '48000', '-ac', '2', '-ab', '128k', '-vcodec', 'libx264', '-preset', 'veryfast', '-framerate', '24', '-f', 'flv', input_url])


def run_channel():
    channel_creation()
    rtmp_stream()

def stop_process():
    quit = keyboard.press_and_release('q')

    stop_channel = client.stop_channel(
        ChannelId=Id_of_channel
    )

    stop_waiter = client.get_waiter('channel_stopped')

    stop_waiter.wait(
        ChannelId=Id_of_channel,
        WaiterConfig={
            'Delay':25,
            'MaxAttempts':6
        },
    )

    delete_channel = client.delete_channel(
        ChannelId=Id_of_channel
    )

    channel_delete_waiter = client.get_waiter('channel_deleted')

    channel_delete_waiter.wait(
        ChannelId=Id_of_channel,
        WaiterConfig={
            'Delay':25,
            'MaxAttempts':6
        },
    )

    delete_input = client.delete_input(
        InputId=input_id
    )


t = threading.Timer(3600, stop_process)


create_channel_input()
t.start()
run_channel()
