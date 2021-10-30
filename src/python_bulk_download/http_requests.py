from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.pool import ThreadPool as thread_pool
from time import time, sleep
from random import uniform as random_float
from sys import getsizeof

def make_http_requests(urls,bandwidth_utilisation=-1,threads=32):
    global cooldown
    cooldown = set()
    global start_times
    start_times = [time()]
    global download_sizes
    download_sizes = [0]
    global max_bandwidth_usage
    max_bandwidth_usage = bandwidth_utilisation
    if hasattr(urls, '__iter__') and not isinstance(urls,str):
        return _multithread(function=_make_http_request,arguments=urls,pool_size=threads)
    else:
        return _make_http_request(urls)

def _make_http_request(url):
    global cooldown
    global start_times
    global download_sizes
    global max_bandwidth_usage
    if not (url.startswith("https://") or url.startswith("http://")):
        url = "https://"+url
    base_url = url.split("/")[0]
    response = None
    download_size = 0
    tries = 4
    i = 0
    sleep(random_float(0,1))
    while 0 < max_bandwidth_usage*0.95 < sum(download_sizes*8)/(time()-start_times[-1])/pow(10,6):
        sleep(random_float(0,1))
    while i < tries:
        i += 1
        successful = False
        while base_url in cooldown:
            sleep(random_float(0,0.1))
        try:
            response_obj = urlopen(url,timeout=10)
            response = response_obj.read().decode("utf-8")
            successful = True
        except:
            try:
                raise
            except HTTPError as e:
                response_obj = e
            except URLError as e:
                response_obj = type("", (), dict(code=-1,reason=str(e.reason),headers=type("", (), dict(items=lambda x: (),get=lambda x,y,z: "0"))()))()
            except Exception as e:
                response_obj = type("", (), dict(code=-2,reason=str(e),headers=type("", (), dict(items=lambda x: (),get=lambda x,y,z: "0"))()))()
            finally:
                if response_obj.code == 429:
                    i -= 1
                    if base_url not in cooldown:
                        cooldown.add(base_url)
                        cooldown_timer = 0.01
                        while cooldown_timer < 60:
                            sleep(cooldown_timer)
                            try:
                                response_obj = urlopen(url,timeout=10)
                                response = response_obj.read().decode("utf-8")
                                break
                            except:
                                cooldown_timer *= 2
                        cooldown.discard(base_url)
                        successful = True
        finally:
            line_size = len(response_obj.reason)+15
            header_size = sum(len(key)+len(value)+4 for key,value in response_obj.headers.items())+2
            content_size = max(getsizeof(response)-49, int(response_obj.headers.get("content-length",0)))
            download_size += (line_size+header_size+content_size)*1.10
            if successful:
                break
    if len(start_times) >= 32:
        start_times.pop()
        download_sizes.pop()
    start_times.insert(0,time())
    download_sizes.insert(0,download_size)
    return {"url":url,"response":response}

def _multithread(function,arguments,pool_size):
    threads = max(1,pool_size)
    with thread_pool(threads) as pool:
        results = pool.imap_unordered(function,arguments)
        pool.close()
        pool.join()
    return (result for result in results if result is not None)
