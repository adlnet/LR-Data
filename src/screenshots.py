import sys
import time
import PIL.Image
import couchdb
from PyQt4.QtCore import *
from PyQt4.QtGui import *
from PyQt4.QtWebKit import *


class Screenshot(QWebView):
    def __init__(self):
        self.app = QApplication(sys.argv)
        QWebView.__init__(self)
        self._loaded = False
        self.loadFinished.connect(self._loadFinished)
        self.settings().setAttribute(QWebSettings.PluginsEnabled, True)
        self.settings().setAttribute(QWebSettings.PluginsEnabled, True)
        # self.page().settings().setAttribute( QWebSettings.JavaEnabled, True )

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


def capture(doc_id, url, db):
    s = Screenshot()
    filename = doc_id + '.jepg'
    s.capture(url, filename)
    try:
        i = PIL.Image.open(filename)
        i.thumbnail((400, 320), PIL.Image.ANTIALIAS)
        i.save(doc_id + "-screenshot.jepg")
        with open(doc_id + "-screenshot.jepg", "r") as f:
            db.put_attachment(doc_id, f, "screenshot.jpeg", "image/jpeg")
    except Exception as ex:
        print repr(ex)


if __name__ == "__main__":
    args = sys.argv
    doc_id = args[2]
    url = args[1]
    db_url = args[3]
    db = couchdb.Database(db_url)
    capture(doc_id, url, db)

