import socket
import cv2
import pickle
import struct
import time
import logging
import json
from datetime import datetime
import argparse
import threading
import sys
import signal
import pathlib

def setup_logger(name:str, file_path:str, level = logging.INFO, formatter:logging.Formatter = logging.Formatter(fmt = '%(levelname)s:%(asctime)s: ___ %(message)s',datefmt = '%d/%m/%Y %H:%M:%S')):
    logger = logging.getLogger(name)
    handler = logging.FileHandler(file_path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    now = datetime.now().strftime("%H:%M:%S")
    logger.critical(f"{name}: new run : {now}")
    logger.setLevel(level)
    return logger

def is_socket_closed(sock: socket.socket) -> bool:
    try:
        # this will try to read bytes without blocking and also without removing them from buffer (peek only)
        data = sock.recv(16, socket.MSG_DONTWAIT | socket.MSG_PEEK)
        if len(data) == 0:
            return True
    except BlockingIOError:
        return False  # socket is open and reading from it would block
    except ConnectionResetError:
        return True  # socket was closed for some other reason
    except Exception as e:
        #logger.exception("unexpected exception when checking if a socket is closed")
        return False
    return False

class VideoCapturingException(Exception):
    def __init__(self, description):
        super.__init__("can't start video capturing")
        self.description = description

class SenderExecutionException(Exception):
    def __init__(self, description):
        super.__init__("wrong object parametres")
        self.description = description

class SenderWrongNameException(Exception):
    def __init__(self):
        super.__init__("wrong sender name")

class Sender:
    def __init__(self, name, host="127.0.0.1", port=8899, video=0):
        self.name = name
        self.host = host
        self.port = port
        self.video_stream_descr = video
        self.sock2ser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_addrin = (self.host,self.port)
        self.sock_is_open = False
        self.frame_size = -1
    
    def _update_status(self):
        self.sock_is_open = not is_socket_closed(self.sock2ser)

    def connect(self):
        if not self.sock_is_open:
            self.sock2ser.connect(self.sock_addrin)
        self._update_status()

    def get_vidoe_size(self):
        vid = cv2.VideoCapture(self.video_stream_descr)
        if (vid.isOpened()):
            img, frm = vid.read()
            a = pickle.dumps(frm)
            self.frame_size = len(a)
            vid.release()
            return self.frame_size
        else:
            raise VideoCapturingException("Sender.get_video_size() : can't start video capturing")
            return -1
    
    def send_add_request(self):
        if not self.sock_is_open:
            return
        if self.frame_size < 0:
            self.sock2ser.close()
            self.sock_is_open = False
            raise SenderExecutionException("Sender.send_add_request() : wrogn frame size: please start self.get_video_size() before")
            return
        name_bytes = self.name.encode("utf-8")
        message = struct.pack("Q250s",self.frame_size, name_bytes)
        self.sock2ser.sendall(message)
        
    def read_add_responce(self):
        data = b""
        payload_size = struct.calcsize("?")
        while len(data) < payload_size:
            packet = self.sock2ser.recv(2*256)
            if not packet:
                break
            data += packet
        responce_msg = data[:payload_size]
        data = data[payload_size:]
        msg = struct.unpack("?", responce_msg)[0] #bool
        #print(msg)
        return msg
    
    def send_video_frames(self, stop_event: threading.Event):
        vid = cv2.VideoCapture(self.video_stream_descr)
        frame_count=0
        while(vid.isOpened() and not stop_event.is_set()):
            if len(str(self.video_stream_descr))<3:
                time.sleep(0.2)
            img, frm = vid.read()
            if not img and len(str(self.video_stream_descr))>2:
                vid.set(cv2.CAP_PROP_POS_FRAMES, 0)
                img, frm = vid.read()
            frm = cv2.cvtColor(frm, cv2.COLOR_BGR2RGB)
            kx = 640/frm.shape[1]
            frm = cv2.resize(frm, None, fx=kx, fy=kx, interpolation=cv2.INTER_CUBIC)
            a = pickle.dumps(frm)
            message = a #struct.pack("Q", len(a)) + 
            self.sock2ser.sendall(message)
            
            frame_count+=1
            if (frame_count == 1000):
                frame_count=0
                self._update_status()
                if not self.sock_is_open:
                    vid.release()
                    return
        if stop_event.is_set():
            vid.release()
            self.sock2ser.close()
    
    def run(self, stop_event: threading.Event):
        self.get_vidoe_size()
        self.connect()
        self.send_add_request()
        if not self.read_add_responce():
            self.sock2ser.close()
            self.sock_is_open = False
            raise SenderWrongNameException()
        self.send_video_frames(stop_event) #need new thread? #or brute closing and make check on server
        return self.sock_is_open
    

class Runner:
    def __init__(self):
        self.logger = setup_logger(name = __name__, file_path="runner.1.txt", level=logging.DEBUG)
        #TODO add logger settigns

        self.base_name = "test"
        self.name = self.base_name
        self.host = None
        self.port = None
        self.video_descr = None
        self.sender = None
        self.flag = 0

    def parse_args(self): #TODO add parsing args from cli
        parser = argparse.ArgumentParser(
                    prog='videosender',
                    description='Send video stream to special reciever')
        
        parser.add_argument('-n','--name', type = str,  nargs=1, required=True,  help = 'name for server list, must be unique for server')
        parser.add_argument('-H', '--host', nargs = '?', help = 'host addr, localhost is default')
        parser.add_argument('-p', '--port', type = int, nargs = '?', help = 'server listening port, 8899 is default')           # positional argument
        parser.add_argument('-v', '--video',type = pathlib.Path, nargs = '?', help = 'vide stream descriptor, uint(len<2) or filepath (len>2), 0 (main camera) is default')
        args = parser.parse_args()
        self.base_name = args.name[0]
        self.name = self.base_name
        #print(self.base_name)
        if args.host is not None:
            self.host = args.host
        if args.port is not None:
            self.port = args.port
        if args.video is not None:
            self.video_descr = args.video
            if len(str(self.video_descr))<3:
                self.video_descr = int(str(self.video_descr))
    def make_kargs(self):
        kargs = dict([])
        if self.host is not None:
            kargs["host"] = self.host
        if self.port is not None:
            kargs["port"] = self.port
        if self.video_descr is not None:
            kargs["video"] = self.video_descr
        return kargs
    def run(self, stop_event: threading.Event):
        if(self.flag>0):
            self.name = self.base_name + f" ({self.flag})"
        self.sender = Sender(name = self.name, **self.make_kargs())
        try:
            if stop_event.is_set():
                return
            self.sender.run(stop_event)
        except SenderWrongNameException:
            self.logger.warning("changing name")
            self.flag+=1
            self.run(stop_event)
        except TimeoutError:
            self.logger.warning("conn refused (timeout), reconn")
            time.sleep(10)
            self.run(stop_event)
        except ConnectionAbortedError:
            self.logger.warning("conn aborted (server issues), reconn")
            time.sleep(10)
            self.run(stop_event)
        except ConnectionRefusedError:
            self.logger.warning("conn refused (refused), reconn")
            time.sleep(10)
            self.run(stop_event)
        except SenderExecutionException as ee:
            self.logger.error("some exception on Executional algo")
            self.logger.error(ee.description)
            return
        except VideoCapturingException as vce:
            self.logger.error("some problems with video input")
            self.logger.error(vce.description)
            return


class MyInterrupter:
    def __init__(self):
        self.stop_event = threading.Event()
    def get_event(self)->threading.Event:
        return self.stop_event
    def interrupt_handler(self,signum, frame)->None:
        self.stop_event.set()
        sys.exit(0)

if(__name__ == "__main__"):
    interrupter = MyInterrupter()
    signal.signal(signal.SIGINT, interrupter.interrupt_handler)
    runner = Runner()
    runner.parse_args()
    runner.run(interrupter.get_event())