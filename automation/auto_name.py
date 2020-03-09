from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
#pip install watchdog for these packages to work

import os
import jsong
import time

class MyHandler(FileSystemEventHandler):
    i = 1
    def on_modified(self, event):
        new_name = "new_screenshot" + str(self.i) + ".png"
        for filename in os.listdir(folder_to_track):
            file_exists = os.path.isfile(folder_destination + "/" + new_name)
            while file_exists:
                self.i += 1
                new_name = "new_screenshot" + str(self.i) + ".png"
                file_exists = os.path.isfile(folder_destination + "/" + new_name)
            src = folder_to_track + "/" + filename
            new_destination = folder_destination + "/" + new_name
            os.rename(src, new_destination)


folder_to_track = "/Users/dickysentosa/Desktop/MyFolder"
folder_destination = "/Users/dickysentosa/Desktop/NewFolder"
event_handler = MyHandler()
observer = Observer()
observer.schedule(event_handler, folder_to_track, recursive=True)
observer.start()

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    observer.stop()
observer.join()