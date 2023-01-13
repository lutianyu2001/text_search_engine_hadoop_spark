import requests
import threading


class myThread(threading.Thread):
    def __init__(self, threadID, name, fro, end, num):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.fro = fro
        self.end = end
        self.num = num

    def run(self) -> None:
        print("Start thread " + self.name)
        self.spider_thread()
        print("End thread " + self.name)

    def spider_thread(self):
        for i in range(self.fro, self.end):
            if i % self.num != self.threadID:
                continue
            url = "https://www.gutenberg.org/cache/epub/" + i.__str__() + "/pg" + i.__str__() + ".txt"
            try:
                r = requests.get(url)
                r.encoding = r.apparent_encoding
                if not r.ok:
                    print("The " + i.__str__() + " file: Error")
                    # print(r.text)
                # elif "Language: English" not in r.text:
                #     print("The " + i.__str__() + " file: Language is not English")
                else:
                    filename = "./data/" + i.__str__() + ".txt"
                    f = open(filename, "w", encoding="utf-8")
                    f.write(r.text)
                    f.close()
                    print("The " + i.__str__() + " file: Downloaded")
                    # k = k + 1
                # time.sleep(10)
            except BaseException:
                print("The " + i.__str__() + " file: Error")
                continue


class spiderThreads(object):
    def __init__(self, num, fro, end):
        self.num = num
        self.fro = fro
        self.end = end

    def start(self):
        threads = []
        for i in range(0, self.num):
            thread = myThread(i, "Thread-" + i.__str__(), self.fro, self.end, self.num)
            threads.append(thread)
        for i in range(0, self.num):
            threads[i].start()
        for i in range(0, self.num):
            threads[i].join()


if __name__ == '__main__':
    # test request url
    # url = "https://www.gutenberg.org/cache/epub/1004/pg1004.txt"
    # r = requests.get(url)
    # r.encoding = r.apparent_encoding
    # print(r.ok)
    # # print(r.text)
    # filename = "./data/test.txt"
    # f = open(filename, "w", encoding="utf-8")
    # f.write(r.text)
    # f.close()
    st = spiderThreads(50, 1000, 10000)
    st.start()
