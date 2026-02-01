import matplotlib.pyplot as plt
import numpy as np
import torch
import torchvision.transforms as transforms
from torchvision import models, datasets
import ultralytics
from ultralytics import YOLO
from flask import Flask, render_template, Response, url_for, send_file
import argparse
import cv2
import pickle
import socket
import struct
import time
import logging
import json
from datetime import datetime
import threading
import signal
import sys
import queue
from collections import defaultdict
import os
import io
import zipfile

class ServerNN: pass
class ServerReciever: pass
class ServerSender: pass

def setup_logger(name:str, file_path:str, level = logging.INFO, formatter:logging.Formatter = logging.Formatter(fmt = '%(levelname)s:%(asctime)s: ___ %(message)s',datefmt = '%d/%m/%Y %H:%M:%S'))->logging.Logger:
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


def get_ip_address()->str:
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect(("8.8.8.8", 80))
  return s.getsockname()[0] #0 - addr - str, 1 - port- int


def framedelay(vidFile: cv2.VideoCapture):
    fps = vidFile.get(cv2.cv.CV_CAP_PROP_FPS)
    waitframe = 1/fps
    return waitframe


class MyInterrupter:
    def __init__(self):
        self.stop_event = threading.Event()
    def get_event(self)->threading.Event:
        return self.stop_event
    def interrupt_handler(self,signum, frame)->None:
        self.stop_event.set()
        sys.exit(0)


class Timer:
    def __init__ (self):
        self.start = time.perf_counter() #sec
    def restart(self):
        self.start = time.perf_counter()
    def get_time(self):
        return time.perf_counter() - self.start


class EndpointAction(object):
    def __init__(self, action):
        self.action = action
        self.response = Response(status=200, headers={})

    def __call__(self, **kargs):
        res = self.action(**kargs)
        return res


class element:
    def __init__(self,url,name):
        self.url = url
        self.name = name


class StatStruct:
    def __init__(self):
        self.set_pers = set()
        self.set_skate = set()
        self.set_skaters = set()
        self.on_screne_stat = None #as using polling from client - we don't need cashing queue - just a single tuple


class DataSource:
    base_file_path = ".\\"#"G:\\pol3\\6sem\\"
    def __init__(self,name: str, sock:socket, size:int):
        self.in_queue = queue.Queue()
        self.out_queue = queue.Queue()
        self.name = name
        self.dump_size = size
        self.sock_in = sock
        self.is_open = True
        self.has_nn_thread = False
        self.stat = StatStruct()
        self.track_history = defaultdict(lambda: [])
        self.timer_in = 1 #sec
        self.timer_out = 1 #sec


class ServerReciever:
    def __init__(self, source_dict:dict[str,DataSource], port=8899):
        self.port = port
        self.ip_addr = "0.0.0.0"#get_ip_address()
        self.sock_addrin = (self.ip_addr,self.port)
        self.client_threads = []
        self.source_dict:dict[str,DataSource] = source_dict
    def client_working_loop(data_obj: DataSource, stop_event:threading.Event) -> None:
        frame_count = 0
        timer = Timer()
        while not stop_event.is_set():
            data_buf=b""
            payload_size = data_obj.dump_size
            data_buf = data_obj.sock_in.recv(payload_size)
            frame = pickle.loads(data_buf)
            data_obj.in_queue.put(frame)
            frame_count+=1
            if(frame_count%10 == 0.0):
                data_obj.timer_in = timer.get_time()/10.0
                timer.restart()
            if(frame_count  == 1000 ):
                frame_count=0
                is_close = is_socket_closed(data_obj.sock_in)
                data_obj.is_open = not is_close
        data_obj.sock_in.close()
            
    def read_hello_struct(self,client:socket)->tuple[str,int]:
        data = b""
        payload_size = struct.calcsize("Q250s")
        while len(data) < payload_size:
            packet = client.recv(4096)
            if not packet:
                break
            data += packet
        msg_struct = struct.unpack("Q250s", data)
        name = msg_struct[1].decode("utf-8").strip("\0\n \t\x00")
        print(name)
        frame_size = msg_struct[0]
        return (name,frame_size)
    def check_unique(self,name:str)->bool:
        result = False
        if name in self.source_dict:
            result = False
        else:
            result = True
        return result
    def send_add_responce(self, client:socket, result:bool)->None:
        message = struct.pack("?", result)
        client.sendall(message)
    def start_client_thread(self, data: DataSource)->None:
        self.source_dict[data.name] = data
        print(f"new client added: {data.name}", flush=True)
        new_client_thread = threading.Thread(target = ServerReciever.client_working_loop, args=[data, self.stop_event])
        new_client_thread.start()
        self.client_threads.append(new_client_thread)
    def run(self, stop_event:threading.Event) -> None:
        self.stop_event = stop_event
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as self.sock_listener:
            self.sock_listener.settimeout(1000)
            self.sock_listener.bind(self.sock_addrin)
            self.sock_listener.listen(5)
            while not self.stop_event.is_set():
                client, addr = self.sock_listener.accept()
                name, frame_size = self.read_hello_struct(client)
                is_unique = self.check_unique(client)
                self.send_add_responce(client,is_unique)
                if(is_unique):
                    data_struct = DataSource(name, client, frame_size)
                    self.start_client_thread(data_struct)
            self.sock_listener.shutdown(socket.SHUT_RDWR)
            self.sock_listener.close()


class ServerSender:
    def __init__(self, source_dict:dict[str,DataSource]):
        self.app: Flask = Flask(__name__)
        self.source_dict:dict[str,DataSource] = source_dict
    
    def home(self):
        data = []
        context = {}
        for name in self.source_dict.keys():
            url = url_for("video_stream", name=name)
            if self.source_dict[name].is_open and not is_socket_closed(self.source_dict[name].sock_in):
                data.append(element(url,name))
        context['data'] = data
        page = render_template("index.html", **context)
        return Response(page)
    
    def video_feed(self, name:str):
        return Response(self.gen_video(name),mimetype='multipart/x-mixed-replace; boundary=--frame')
    
    def download_folder_as_zip(self, name:str):
        # The path to the folder you want to zip (e.g., in your static directory)
        folder_path = DataSource.base_file_path + f"result\\{name}\\"
        # Create an in-memory byte stream
        memory_file = io.BytesIO()
        timestr = time.strftime("%Y%m%d-%H%M%S")
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the directory and add files to the zip
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    # The arcname parameter ensures the file path within the zip 
                    # is relative to the base folder, not the absolute path on the server
                    zipf.write(file_path, arcname=os.path.relpath(file_path, folder_path))
        # Crucial step: move the file pointer back to the start of the stream
        memory_file.seek(0)
        return send_file(memory_file,
                     download_name="skaters_from_"+name+"_"+timestr+".zip",
                     as_attachment=True)

    def gen_video(self, name:str):
        prefix = "\r\n--frame\r\n" + "Content-Type: image/jpeg\r\n\r\n"
        prefix = prefix.encode("utf-8")
        postfix = "\r\n"
        postfix = postfix.encode("utf-8")
        self.source_dict[name].out_queue = queue.Queue()
        time.sleep(0.1)
        while not self.stop_event.is_set():
            frame = self.source_dict[name].out_queue.get()
            yield (prefix + frame + postfix)
            time.sleep(0.1)
    
    def gen_stat(self, name:str):
        prefix = ""
        postfix = ""
        context = dict()
        with self.app.app_context():
            if not self.stop_event.is_set():
                on_screne_tuple = self.source_dict[name].stat.on_screne_stat
                context["on_screne"] = on_screne_tuple
                total_pers = len(self.source_dict[name].stat.set_pers)
                total_skate = len(self.source_dict[name].stat.set_skate)
                total_skaters = len(self.source_dict[name].stat.set_skaters)
                context["total"] = (total_pers,total_skate,total_skaters)
                context["mean_spf_in"] = self.source_dict[name].timer_in# + self.source_dict[name].timer_in
                context["mean_spf_out"] = self.source_dict[name].timer_out# + self.source_dict[name].timer_in
                context["mean_spf_all"] = self.source_dict[name].timer_out + self.source_dict[name].timer_in
                stat = render_template("stats_templ.html", **context)
                return Response((prefix + stat + postfix).encode("utf-8"), mimetype="text/html")
                time.sleep(0.1)
        
    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None):
        self.app.add_url_rule(endpoint, endpoint_name, EndpointAction(handler))
    
    def video_stream(self, name:str):
        context = {}
        context["name"] = name
        context["url_home"] = url_for("home")
        context["video"] = url_for("video_feed", name=name)
        context["mean_spf"] = self.source_dict[name].timer_out# + self.source_dict[name].timer_in
        context["url_stat"] = url_for("video_stat", name=name)
        context["download"] = url_for("download_res",name=name)
        page = render_template("video_feed.html", **context)
        return Response(page)

    def video_stat(self, name:str):
        context = {}
        context["name"] = name
        context["url_home"] = url_for("home")
        context["url_stream"] = url_for("video_stream", name=name)
        context["stats"] = url_for("stat_feed", name=name)
        context["download"] = url_for("download_res",name=name)
        total_pers = len(self.source_dict[name].stat.set_pers)
        total_skate = len(self.source_dict[name].stat.set_skate)
        total_skaters = len(self.source_dict[name].stat.set_skaters)
        context["total_freeze"] = (total_pers,total_skate,total_skaters)
        page = render_template("video_stats.html", **context)
        return Response(page)
    
    def prepare_routes(self):
        self.add_endpoint(endpoint="/video_feed/<string:name>", endpoint_name="video_feed", handler=self.video_feed)
        self.add_endpoint(endpoint="/stat_feed/<string:name>", endpoint_name="stat_feed", handler=self.gen_stat)
        self.add_endpoint(endpoint="/video_stream/<string:name>", endpoint_name="video_stream", handler=self.video_stream)
        self.add_endpoint(endpoint="/video_stats/<string:name>", endpoint_name="video_stat", handler=self.video_stat)
        self.add_endpoint(endpoint="/download/<string:name>", endpoint_name="download_res", handler=self.download_folder_as_zip)
        self.add_endpoint(endpoint="/", endpoint_name="home", handler=self.home)
    
    def run(self,stop_event:threading.Event):
        self.stop_event = stop_event
        self.app.run(host='0.0.0.0',port = 45000, debug=False)


class ServerNN:
    def __init__(self, source_dict: dict[str,DataSource]):
        self.source_dict: dict[str,DataSource] = source_dict
        self.source_threads = []
        self.base_file_path = DataSource.base_file_path
    
    def show_tracking(self, name, results):
        if len(results)<1:
            return None
        result = results[0]
        if result.boxes and result.boxes.is_track:
            boxes = result.boxes.xywh.cpu()
            track_ids = result.boxes.id.int().cpu().tolist()
            # Visualize the result on the frame
            frame = result.plot(font_size = 3, line_width = 1) 
            # Plot the tracks
            for box, track_id in zip(boxes, track_ids):
                x, y, w, h = box
                track = self.source_dict[name].track_history[track_id]
                track.append((float(x), float(y)))  # x, y center point
                if len(track) > 60:  # retain 30 tracks for 30 frames
                    track.pop(0)
                # Draw the tracking lines
                points = np.hstack(track).astype(np.int32).reshape((-1, 1, 2))
                cv2.polylines(frame, [points], isClosed=False, color=(255, 0, 0), thickness=1) #error
            return frame
        return result.plot() #in there is no boxes
    
    def nn_res_analyze(self,name:str, nn_res:list):
        flag_pers = False
        flag_skate = False
        count_on_screne_pers = 0
        on_screne_tr_pers = set()
        on_screne_skate = set()
        
        for result in nn_res:
            for box in result.boxes:
                if box.cls[0] != 36.:
                    count_on_screne_pers += 1
                    if box.is_track:
                        #self.source_dict[name].stat.set_pers.add(box.id)
                        on_screne_tr_pers.add(box)
                        flag_pers = True
                     #not a skateboard
                else:
                    flag_skate = True
                    on_screne_skate.add(box)
        on_screne_tr_pers_ids = set()
        for box in on_screne_skate:
            if box.is_track:
                self.source_dict[name].stat.set_skate.add(int(box.id[0]))
        for box in on_screne_tr_pers:
            if box.is_track:
                self.source_dict[name].stat.set_pers.add(int(box.id[0]))
                on_screne_tr_pers_ids.add(int(box.id[0]))
        
        if(flag_skate and flag_pers):
            for pers in on_screne_tr_pers:
                has_nearest_skate = False
                if not has_nearest_skate:
                    for skate in on_screne_skate:
                        if(skate.xyxy[0][0]<pers.xywh[0][0]<skate.xyxy[0][2])and (skate.xyxy[0][1]>pers.xywh[0][1]+pers.xywh[0][3]*0.2):
                            
                            if not int(pers.id[0]) in self.source_dict[name].stat.set_skaters:
                                self.source_dict[name].stat.set_skaters.add(int(pers.id[0]))
                                result = nn_res[0]
                                frame = result.plot(font_size = 3, line_width = 1)
                                output_dir = self.base_file_path+f"result\\{name}\\"
                                if not os.path.exists(output_dir):
                                    os.makedirs(output_dir, exist_ok=True)
                                plt.imsave(output_dir + f"{name}_{int(pers.id[0])}_full.png", frame)
                                frame = frame[int(pers.xyxy[0][1]):int(pers.xyxy[0][3]+1), int(pers.xyxy[0][0]):int(pers.xyxy[0][2]+1),:]
                                plt.imsave(output_dir + f"{name}_{int(pers.id[0])}.png", frame)
                            has_nearest_skate=True
                            break
        on_screne_skaters = self.source_dict[name].stat.set_skaters.intersection(on_screne_tr_pers_ids)
        on_screne_data = (len(on_screne_tr_pers),len(on_screne_skate),len(on_screne_skaters))
        self.source_dict[name].stat.on_screne_stat = on_screne_data
        
    def run_nn(self,name:str)->None:
        thread_nn = YOLO(self.base_file_path + "yolo26n.pt") #for tracking needs owm nn for every thread (datasource)
        timer = Timer()
        frame_counter = 0
        while not self.stop_event.is_set():
            if not self.source_dict[name].is_open:
                return
            nn_input = self.source_dict[name].in_queue.get()
            nn_res=thread_nn.track(nn_input, persist=True, classes = [0,36], conf=0.2, iou=0.6)
            self.nn_res_analyze(name,nn_res)
            nn_out = self.show_tracking(name,nn_res)
            if nn_out is None:
                continue
            nn_out = cv2.cvtColor(nn_out, cv2.COLOR_RGB2BGR)#?
            im_bytes = cv2.imencode(
                ".jpeg",  
                nn_out,
            )[1].tobytes()
            self.source_dict[name].out_queue.put(im_bytes)
            frame_counter+=1
            if(frame_counter == 10):
                frame_counter=0
                self.source_dict[name].timer_out = timer.get_time()/10
                timer.restart()
    
    def check_threads(self)->None:
        for name in self.source_dict:
            data = self.source_dict[name]
            if not data.has_nn_thread:
                self.source_dict[name].has_nn_thread = True
                new_source_thread = threading.Thread(target=self.run_nn,args=[name])
                new_source_thread.start()
                self.source_threads.append(new_source_thread)
    
    def run(self, stop_event:threading.Event):
        self.stop_event = stop_event
        while not self.stop_event.is_set():
            self.check_threads()
            time.sleep(5)
        

class Runner:
    def __init__(self):
        parser = argparse.ArgumentParser(
                    prog='videosender',
                    description='Send video stream to special reciever')
        parser.add_argument('-p', '--port', type = int, nargs = 1, required=True, help = 'server listening port, 8899 is default')           # positional argument
        args = parser.parse_args()
        resv_port = args.port[0]

        self.source_dict: dict[str,DataSource] = dict({})
        self.nn = ServerNN(self.source_dict)
        self.rcv = ServerReciever(self.source_dict,port=resv_port)
        self.sndr = ServerSender(self.source_dict)
    
    def run(self, stop_event:threading.Event):
        self.stop_event = stop_event
        self.rcv_thr = threading.Thread(target=self.rcv.run, args=[self.stop_event])
        self.rcv_thr.start()
        self.nn_thr = threading.Thread(target=self.nn.run, args=[self.stop_event])
        self.nn_thr.start()
        self.sndr.prepare_routes()
        self.sndr.run(self.stop_event) #need main thread


if(__name__ == "__main__"):
    interrupter = MyInterrupter()
    server = Runner()
    signal.signal(signal.SIGINT, interrupter.interrupt_handler)
    server.run(interrupter.get_event())