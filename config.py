import redis

class RedisConnection:
    _instance = None

    @staticmethod
    def get_instance():
        if RedisConnection._instance is None:
            RedisConnection._instance = redis.Redis(host='localhost', port=6379, db=0)
        return RedisConnection._instance

# Usage example
# connection = RedisConnection.get_instance()