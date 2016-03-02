using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net;

namespace Chuye.Kafka.Serialization {
    public class ConnectedSocket : IEquatable<ConnectedSocket> {
        public String Host { get; private set; }
        public Int32 Port { get; private set; }
        public Socket Socket { get; set; }

        public ConnectedSocket(String host, Int32 port, Socket socket) {
            Host = host;
            Port = port;
            Socket = socket;
        }

        public Boolean Equals(ConnectedSocket other) {
            return other != null
                && (Host.Equals(other.Host, StringComparison.Ordinal))
                && (Port == Port);
        }

        public override Boolean Equals(Object obj) {
            return obj != null
                && obj is ConnectedSocket
                && Equals((ConnectedSocket)obj);
        }

        public override Int32 GetHashCode() {
            return (Host != null ? Host.GetHashCode() : 0)
                ^ Port.GetHashCode()
                ^ Socket.GetHashCode();
        }
    }

    public class SocketManager : IDisposable {
        private readonly HashSet<ConnectedSocket> _availableSockets;
        private readonly HashSet<ConnectedSocket> _activeSockets;

        public SocketManager() {
            _availableSockets = new HashSet<ConnectedSocket>();
            _activeSockets = new HashSet<ConnectedSocket>();
        }

        public ConnectedSocket Obtain(String host, Int32 port) {
            ConnectedSocket socket = null;
            foreach (var item in _availableSockets) {
                if (item.Host == host && item.Port == port) {
                    socket = item;
                    break;
                }
            }

            if (socket != null) {
                _availableSockets.Remove(socket);
                _activeSockets.Add(socket);
                return socket;
            }

            socket = new ConnectedSocket(host, port, new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp));
            socket.Socket.Connect(host, port);
            _activeSockets.Add(socket);
            return socket;
        }

        public void Release(ConnectedSocket socket) {
            if (!_activeSockets.Remove(socket)) {
                throw new InvalidOperationException("Anonymous socket release not supported");
            }
            _availableSockets.Add(socket);
        }

        public void Dispose() {
            foreach (var socket in _availableSockets) {
                socket.Socket.Close();
            }
            _availableSockets.Clear();
            foreach (var socket in _activeSockets) {
                socket.Socket.Close();
            }
            _activeSockets.Clear();
        }
    }
}
