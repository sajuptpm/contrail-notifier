import amqp.exceptions
import kombu
import gevent
import time
import socket
from pysandesh.gen_py.process_info.ttypes import ConnectionStatus

class NotificationKombuClientBase(object):

    def connect(self, delete_old_q=False):
        msg = "Connecting to rabbitmq on %s:%s user %s" \
              % (self._rabbit_ip, self._rabbit_port, self._rabbit_user)
        #self._logger(msg, level=SandeshLevel.SYS_NOTICE)
        #self._update_sandesh_status(ConnectionStatus.INIT)
        self._conn_state = ConnectionStatus.INIT
        while True:
            try:
                self._conn = kombu.Connection(hostname=self._rabbit_ip,
                                              port=self._rabbit_port,
                                              userid=self._rabbit_user,
                                              password=self._rabbit_password,
                                              virtual_host=self._rabbit_vhost)

                #self._update_sandesh_status(ConnectionStatus.UP)
                self._conn_state = ConnectionStatus.UP
                msg = 'RabbitMQ connection ESTABLISHED'
                print "=========", msg
                #self._logger(msg, level=SandeshLevel.SYS_NOTICE)

                #if delete_old_q:
                #    bound_q = self._update_queue_obj(self._conn.channel())
                #    try:
                #        bound_q.delete()
                #    except amqp.exceptions.ChannelError as e:
                #        msg = "Unable to delete the old amqp Q: %s" % str(e)
                #        self._logger(msg, level=SandeshLevel.SYS_ERR)

                self._obj_update_q = self._conn.SimpleQueue(self._update_queue_obj)

                #old_subscribe_greenlet = self._subscribe_greenlet
                self._subscribe_greenlet = gevent.spawn(self._notification_subscribe)
                #if old_subscribe_greenlet:
                #    old_subscribe_greenlet.kill()

                break
            except Exception as e:
                if self._conn_state != ConnectionStatus.DOWN:
                    msg = "RabbitMQ connection down: %s" %(str(e))
                    print "=========", msg
                    #self._logger(msg, level=SandeshLevel.SYS_ERR)

                #self._update_sandesh_status(ConnectionStatus.DOWN)
                self._conn_state = ConnectionStatus.DOWN
                time.sleep(2)

    def __init__(self, rabbit_ip, rabbit_port, rabbit_user, rabbit_password,
                 rabbit_vhost, q_name, subscribe_cb):
        self._rabbit_ip = rabbit_ip
        self._rabbit_port = rabbit_port
        self._rabbit_user = rabbit_user
        self._rabbit_password = rabbit_password
        self._rabbit_vhost = rabbit_vhost
        self._subscribe_cb = subscribe_cb
        #self._logger = logger

        obj_upd_exchange = kombu.Exchange('vnc_config.object-update', 'fanout',
                                          durable=False)

        self._update_queue_obj = kombu.Queue(q_name, obj_upd_exchange)
        #self._subscribe_greenlet = None
        self.connect(True)

    def _notification_subscribe(self):
        msg = "Running greenlet _notification_subscribe"
        print "=========", msg
        #self._logger(msg, level=SandeshLevel.SYS_NOTICE)

        with self._conn.SimpleQueue(self._update_queue_obj) as queue:
            while True:
                try:
                    message = queue.get()
                except Exception as e:
                    msg = "Disconnected from rabbitmq. Reinitializing connection: %s" % str(e)
                    print "=========", msg
                    #self._logger(msg, level=SandeshLevel.SYS_WARN)
                    self.connect()
                    # never reached
                    continue

                trace = None
                try:
                    self._subscribe_cb(message.payload)
                except Exception as e:
                    msg = "Subscribe callback had error: %s" % str(e)
                    print "=========", msg
                    #self._logger(msg, level=SandeshLevel.SYS_WARN)
                finally:
                    try:
                        message.ack()
                    except Exception as e:
                        msg = "Disconnected from rabbitmq. Reinitializing connection: %s" % str(e)
                        print "=========", msg
                        #self._logger(msg, level=SandeshLevel.SYS_WARN)
                        self.connect()


class NotificationKombuClient(NotificationKombuClientBase):
    def __init__(self, db_client_mgr, rabbit_ip, rabbit_port, ifmap_db,
                 rabbit_user, rabbit_password, rabbit_vhost):
        #self._db_client_mgr = db_client_mgr
        #self._sandesh = db_client_mgr._sandesh
        #self._ifmap_db = ifmap_db
        #listen_port = db_client_mgr.get_server_port()
        q_name = 'notification.%s-%s' %(socket.gethostname())
        super(NotificationKombuClient, self).__init__(
            rabbit_ip, rabbit_port, rabbit_user, rabbit_password, rabbit_vhost,
            q_name, self._notification_subscribe_callback)

        #self._publish_queue = Queue()
        #self._dbe_publish_greenlet = gevent.spawn(self._dbe_oper_publish)

    def notification_publish(self, message):
    	"""
    		c from contrail-controller/src/config/api-server/vnc_cfg_ifmap.py
    		def dbe_create(self, obj_type, obj_ids, obj_dict):
    		def dbe_update(self, obj_type, obj_ids, new_obj_dict):
    		def dbe_delete(self, obj_type, obj_ids, obj_dict):
    		def ref_update(self, obj_type, obj_uuid, ref_type, ref_uuid, ref_data, operation):
    	"""
        #self._db_client_mgr.wait_for_resync_done() <=====Check it, imp
        try:
            while True:
                try:
                    self._obj_update_q.put(message, serializer='json')
                    break
                except Exception as e:
                    log_str = "Disconnected from rabbitmq. Reinitializing connection: %s" % str(e)
                    print "=========", log_str
                    #self.config_log(log_str, level=SandeshLevel.SYS_WARN)
                    time.sleep(1)
                    self.connect()
        except Exception as e:
            log_str = "Unknown exception in _dbe_oper_publish greenlet" + str(e)
            print "=========", log_str
            #self.config_log(log_str, level=SandeshLevel.SYS_ERR)
            time.sleep(1)





