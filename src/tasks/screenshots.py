import sys
import time
import urllib2
import uuid
import Image
import PIL
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4.QtWebKit import *

class Screenshot(QWebView):
    def __init__(self):
        self.app = QApplication(sys.argv)
        QWebView.__init__(self)
        self._loaded = False
        self.loadFinished.connect(self._loadFinished)

    def capture(self, url, output_file):
        self.load(QUrl(url))
        self.wait_load()
        # set to webpage size
        frame = self.page().mainFrame()
        self.page().setViewportSize(QSize(1280, 1024))
        # render image
        image = QImage(self.page().viewportSize(), QImage.Format_ARGB32)
        painter = QPainter(image)
        frame.render(painter)
        painter.end()
        image.save(output_file)

    def wait_load(self, delay=0):
        # process app events until page loaded
        while not self._loaded:
            self.app.processEvents()
            time.sleep(delay)
        self._loaded = False

    def _loadFinished(self, result):
        self._loaded = True


if __name__ == "__main__":
    args = sys.argv
    s = Screenshot()
    filename = args[2] + '.jpeg'
    s.capture(args[1], filename)
    i = PIL.Image.open(filename)
    i.thumbnail((400, 320), Image.ANTIALIAS)
    i.save(args[2]+"-screenshot.jpeg")
    i.thumbnail((400, 300), Image.ANTIALIAS)
    i.save(args[2]+"-thumbnail.jpeg")
    print(filename)

