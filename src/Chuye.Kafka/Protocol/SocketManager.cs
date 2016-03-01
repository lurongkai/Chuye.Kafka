using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net;

namespace Chuye.Kafka.Protocol {
    public class SocketManager : IDisposable {
        private readonly HashSet<Socket> _availableSockets;
        private readonly HashSet<Socket> _activeSockets;

        public SocketManager() {
            _availableSockets = new HashSet<Socket>();
            _activeSockets = new HashSet<Socket>();
        }

        public Socket Obtain(IPEndPoint endPoint) {
            Socket socket = null;
            foreach (var item in _availableSockets) {
                if (item.RemoteEndPoint == null || item.RemoteEndPoint.Equals(endPoint)) {
                    break;
                }
            }

            if (socket != null) {
                _availableSockets.Remove(socket);
                _activeSockets.Add(socket);
                return socket;
            }

            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _activeSockets.Add(socket);
            return socket;
        }

        public void Release(Socket socket) {
            _activeSockets.Remove(socket);
            _availableSockets.Add(socket);
        }

        public void Dispose() {
            foreach (var socket in _availableSockets) {
                socket.Close();
            }
            _availableSockets.Clear();
            foreach (var socket in _activeSockets) {
                socket.Close();
            }
            _activeSockets.Clear();
        }
    }
}
