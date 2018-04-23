from multiprocessing import Process, Manager, cpu_count
import webkit_server
import dryscrape

from sa.database import Session
from sa.logger import LOGGER

def get_html(urlQ, callback, xpath_hooks):
    """
    This page takes a url from the URL Queue (urlQ) and
    calls a callbac that will handle the page source.

    xpage_hooks is a list used to determine when the page is loaded,
    see the docs for more details (e.g. ["//div[@data-test='whatever']"] ).
    """
    svr = webkit_server.Server()
    svrconn = webkit_server.ServerConnection(server=svr)
    driver = dryscrape.driver.webkit.Driver(connection=svrconn)

    sess = dryscrape.Session(driver=driver)
    sess.set_header("User-Agent", "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36")
    sess.set_attribute("auto_load_images", False)

    valid_page_func = lambda: any(sess.at_xpath(xpath) for xpath in xpath_hooks)
    session = Session()

    while not urlQ.empty():
        url = urlQ.get()

        try:
            sess.visit(url)
        except webkit_server.InvalidResponseError:
            LOGGER.error("Got invalid response from something? Skipping {}".format(url))
            continue

        try:
            sess.wait_for(valid_page_func, interval=1, timeout=15)
        except dryscrape.mixins.WaitTimeoutError:
            LOGGER.error("Timeout so skipping {}".format(url))
            continue

        response = sess.body()
        callback(session, url, response)
        sess.reset()

    svr.kill()
    session.close()

class JSPageScraper():
    def __init__(self, callback, xpath_hooks, table_name, nproc = None):
        self.callback = callback
        self.xpath_hooks = xpath_hooks
        self.table_name = table_name
        self.nproc = max(1, cpu_count()-1) if nproc is None else nproc

    def go(self, urls):
        LOGGER.info("Preparing threads...")

        manager = Manager()
        urlQ = manager.Queue()
        for url in urls:
            urlQ.put(url)

        procs = [Process(target=get_html, args=(urlQ, self.callback, self.xpath_hooks), daemon=True) for i in range(self.nproc)]

        LOGGER.info("Threads started. Fetching n' parsing!")
        for proc in procs:
            proc.start()

        for proc in procs:
            proc.join()

if __name__ == "__main__":
    def dic_parse(db, url, html):
        LOGGER.notice("Got url {} and html with length {}".format(url, len(html)))

    xpath_hooks = ["//div[@data-test='qsp-statistics']", "//div[@data-test='unknown-quote']"]
    jsps = JSPageScraper(dic_parse, xpath_hooks, "key_statistics")

    urls = ["https://ca.finance.yahoo.com/quote/IMO.TO/key-statistics"]
    jsps.go(urls)
