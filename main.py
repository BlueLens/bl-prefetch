from __future__ import print_function
import os
import pickle
import time
import redis
import numpy as np
from multiprocessing import Process
from stylelens_search_vector.vector_search import VectorSearch
from stylelens_object.objects import Objects
from stylelens_image.images import Images
from bluelens_log import Logging

REDIS_USER_OBJECT_HASH = 'bl:user:object:hash'
REDIS_USER_OBJECT_QUEUE = 'bl:user:object:queue'
REDIS_USER_IMAGE_HASH = 'bl:user:image:hash'
REDIS_FEED_IMAGE_HASH = 'bl:feed:image:hash'

VECTOR_SIMILARITY_THRESHHOLD = 700

REDIS_SERVER = os.environ['REDIS_SEARCH_SERVER']
REDIS_PASSWORD = os.environ['REDIS_SEARCH_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

DB_IMAGE_HOST = os.environ['DB_IMAGE_HOST']
DB_IMAGE_PORT = os.environ['DB_IMAGE_PORT']
DB_IMAGE_NAME = os.environ['DB_IMAGE_NAME']
DB_IMAGE_USER = os.environ['DB_IMAGE_USER']
DB_IMAGE_PASSWORD = os.environ['DB_IMAGE_PASSWORD']

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

rconn = redis.StrictRedis(REDIS_SERVER, decode_responses=False, port=6379, password=REDIS_PASSWORD)
options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-prefetch')

vector_search = None
object_api = None
image_api = None

def get_images_from_objects(objects, limit=10):
  global vector_search
  global object_api
  global image_api
  image_ids = []

  for obj in objects:
    image_ids.append(obj['image_id'])

  ids = list(set(image_ids))
  try:
    start_time = time.time()
    _images = image_api.get_images_by_ids(ids)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    images = []
    i = 1
    for image in _images:
      if i > limit:
        break
      image['id'] = str(image.pop('_id'))
      image.pop('images', None)
      images.append(image)
      i = i + 1

    return images
  except Exception as e:
    log.error(str(e))
    return None

  return images

def get_images_by_vector(vector, limit=10):
  global vector_search
  global object_api
  global image_api
  try:
    # Query to search vector
    start_time = time.time()

    vector_d, vector_i = vector_search.search(vector, limit)
    distances = np.fromstring(vector_d, dtype=np.float32)
    ids = np.fromstring(vector_i, dtype=np.int)

    elapsed_time = time.time() - start_time
    log.debug('vector search time: ' + str(elapsed_time))
    # pprint(api_response)
  except Exception as e:
    log.error("Exception when calling SearchApi->search_vector: %s\n" % e)

  arr_i = []
  i = 0
  for d in distances:
    print(d)
    if d <= VECTOR_SIMILARITY_THRESHHOLD:
      if i < limit:
        arr_i.append(ids[i])
      else:
        break
      i = i + 1

  if len(arr_i) > 0:
    ids = [int(x) for x in arr_i]

    try:
      start_time = time.time()
      objects = object_api.get_objects_by_indexes(ids)
      elapsed_time = time.time() - start_time
      print(elapsed_time)
      images = get_images_from_objects(objects)
      return images
    except Exception as e:
      log.error('Trying Objects.get_objects_by_indexes():' + str(e))
      return None

def prefetch_object(rconn):
  global vector_search
  global object_api
  global image_api

  vector_search = VectorSearch()
  object_api = Objects()
  image_api = Images()
  while True:
    key, value = rconn.blpop([REDIS_USER_OBJECT_QUEUE])
    if value is not None:
      object_id = value.decode('utf-8')
      o = rconn.hget(REDIS_USER_OBJECT_HASH, object_id)
      object = pickle.loads(o)
      vector = object['feature']
      if vector is not None:
        images = get_images_by_vector(vector, limit=15)
        if images is not None:
          rconn.hset(REDIS_USER_OBJECT_HASH, object_id, pickle.dumps(images))

if __name__ == '__main__':
  try:
    log.info("start bl-prefetch:1")
    Process(target=prefetch_object, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
