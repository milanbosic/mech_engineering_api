import firebase_admin
from django.http import HttpResponseServerError, HttpResponse, JsonResponse
import json
from firebase_admin import credentials, db, messaging
from django.views.decorators.csrf import csrf_exempt
import threading
from unidecode import unidecode

cred = credentials.Certificate(
    '/home/milan/Downloads/mech-engineering-notifications-firebase-adminsdk-wdzn4-4760a2e9da.json')
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://mech-engineering-notifications.firebaseio.com/'
})

threads = []


def subscribe_to_topics(user):
    for idx, topic in enumerate(user.topics):
        # print('Subscribing to topic:' + topic)
        thread = ThreadSub(idx, 'Thread ' + str(idx), user.token, topic)
        threads.append(thread)


def unsubscribe_from_topics(token, topics):
    for idx, topic in enumerate(topics):
        thread = ThreadUnsub(idx, 'Thread ' + str(idx), token, topic)
        threads.append(thread)


class User:
    def __init__(self, token, topics):
        self.token = token
        self.topics = topics


class ThreadSub(threading.Thread):
    def __init__(self, thread_id, name, token, topic):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.token = token
        self.topic = topic

    def run(self):
        print('Subscribing to ' + self.topic)
        messaging.subscribe_to_topic(self.token, '/topics/' + self.topic)


class ThreadUnsub(threading.Thread):
    def __init__(self, thread_id, name, token, topic):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.token = token
        self.topic = topic

    def run(self):
        print('Unsubscribing from ' + self.topic)
        messaging.unsubscribe_from_topic(self.token, '/topics/' + self.topic)


class ThreadWriteToDB(threading.Thread):
    def __init__(self, thread_id, name, user_ref, idx, topic):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.user_ref = user_ref
        self.idx = idx
        self.topic = topic

    def run(self):
        # print('Writing to database ' + self.topic)
        self.user_ref.update({
            self.idx: self.topic
        })


@csrf_exempt
def req_api(request):
    if request.method == 'POST':
        # JSON object from request data
        json_data = json.loads(request.body)

        topic_list = []
        for idx in range(len(json_data) - 1):
            topic_list.append(
                unidecode(json_data[('topics[' + str(idx) + ']')].replace('(', '').replace(')', '').replace(' ', "_")))
        print('Receiving request...')
        # json_data = json.loads(json.dumps(request.data))
        user = User('', '')
        ref = db.reference('/')
        try:
            user = User(json_data[('token')], topic_list)

        except KeyError:
            HttpResponseServerError("Malformed data!")
        # If the array of subjects from the POST request is empty
        # unsubscribe the token from everything
        user_ref = db.reference(user.token + '/topics')

        if not user.topics:
            # Old topics in JSON format ('id':'topic_name')
            print('Empty list sent, unsubscribe from everything...')
            old_topics_list = user_ref.get()
            unsubscribe_from_topics(user.token, old_topics_list)
            for thread in threads:
                thread.start()

            for t in threads:
                t.join()
            # print('Unsub finished')
            threads.clear()
            # Delete entries from DB
            user_ref.delete()
            return JsonResponse({'message': "Success"})
        else:
            result = ref.child(user.token).get()
            # Token not in DB, no need to unsubscribe, subscribe and add subjects to DB
            if result is None:
                print('Token not found in DB')
                # First insert the token of the new user into DB
                ref.update({
                    user.token: {
                        'topics': {

                        }
                    }
                })
                # Subscribe to all topics the user just sent
                subscribe_to_topics(user)
                for thread in threads:
                    thread.start()

                # Then update the token with a list of topics he'd just subscribed to
                for idx, topic in enumerate(user.topics):
                    thread = ThreadWriteToDB(1, 'Thread ' + str(idx), user_ref, idx, topic)
                    thread.start()
                    threads.append(thread)

                for t in threads:
                    t.join()

                threads.clear()
                print('Database write finished')
                return JsonResponse({'message': "Success"})

            else:
                print('Token in DB')
                # Token exists, unsubscribe from everything and
                # subscribe to the new subjects; update DB.
                old_topics_list = user_ref.get()
                if old_topics_list:
                    unsubscribe_from_topics(user.token, old_topics_list)
                    for thread in threads:
                        thread.start()

                    for t in threads:
                        t.join()
                    # print('Unsub finished')
                    threads.clear()

                user_ref.delete()
                threadLock = threading.Lock()
                subscribe_to_topics(user)
                for thread in threads:
                    thread.start()

                for idx, topic in enumerate(user.topics):
                    thread = ThreadWriteToDB(1, 'Thread ' + str(idx), user_ref, idx, topic)
                    thread.start()
                    threads.append(thread)

                for t in threads:
                    t.join()

                print('Database write finished')
                threads.clear()

                return JsonResponse({'message': "Success"})
