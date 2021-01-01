import threading
import time

lock = threading.Lock()

d = {}


class Key_value:

    def active(self, n, key, value=0, timeout=0):
        if n == 1:
            lock.acquire()
            self.create(key, value, timeout)
            lock.release()

        elif n == 2:
            lock.acquire()
            self.read(key)
            lock.release()

        elif n == 3:
            lock.acquire()
            self.delete(key)
            lock.release()

    def create(self, key, value, timeout=0):
        if key in d:
            print("Key already exists")

        else:
            if key.isalpha():
                # constraints for file size less than 1GB and Json object value less than 16KB
                if len(d) < (1024 * 1020 * 1024) and value <= (16 * 1024 * 1024):
                    if timeout == 0:
                        l = [value, timeout]
                    else:
                        l = [value, time.time() + timeout]
                    if len(key) <= 32:  # constraints for input key_name capped at 32chars
                        d[key] = l
                else:
                    print("error: Memory limit exceeded!! ")  # error message2
            else:
                print(
                    "error: Invalid key_name!! key_name must contain only alphabets and no special characters or numbers")

    def read(self, key):

        if key not in d:
            # error message4
            print("error: given key does not exist in database. Please enter a valid key")
        else:
            b = d[key]
            if b[1] != 0:
                if time.time() < b[1]:  # comparing the present time with expiry time
                    # to return the value in the format of JasonObject i.e.,"key_name:value"
                    string1 = str(key) + ":" + str(b[0])
                    print(string1)
                    return string1
                else:
                    print("error: time-to-live of", key,
                          "has expired")  # error message5
            else:
                string1 = str(key) + ":" + str(b[0])
                print(string1)
                return string1

    def delete(self, key):
        if key not in d:
            # error message4
            print("error: given key does not exist in database. Please enter a valid key")
        else:
            b = d[key]
            if b[1] != 0:
                if time.time() < b[1]:  # comparing the current time with expiry time
                    del d[key]
                    print("key is successfully deleted")
                else:
                    print("error: time-to-live of", key,
                          "has expired")  # error message5
            else:
                del d[key]
                print("key is successfully deleted")


a = Key_value()


numThreads = 1000
threads = []


for _ in range(numThreads):
    print(d)

    n = int(
        input("Press 1 to Create,  2-read,  3-Delete & press any other key for EXIT "))
    if n == 1:
        k = input("Enter Key : ")
        p = int(input("Enter Value : "))
        t = int(input("Enter Timeout if any : "))

    elif n == 2:
        q = input("Enter Key to Read : ")

    elif n == 3:
        q = input("Enter Key to Delete : ")
    else:
        break

    t = threading.Thread(
        target=(a.active), args=(n, k, p, t))

    t.start()
    time.sleep(2)
    threads.append(t)
    print(d)
    print(threads)


for thread in threads:

    thread.join()