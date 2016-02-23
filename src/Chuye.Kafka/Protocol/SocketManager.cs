using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Chuye.Kafka.Protocol {
    public class SocketManager : IDisposable {
        private readonly Stack<Socket> _availableSockets;
        private readonly HashSet<Socket> _activeSockets;

        public SocketManager() {
            _availableSockets = new Stack<Socket>();
            _activeSockets = new HashSet<Socket>();
        }

        public Socket Obtain() {
            Socket socket;
            if (_availableSockets.Count > 0) {
                socket = _availableSockets.Pop();
            }
            else {
                socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            }
            _activeSockets.Add(socket);
            return socket;
        }

        public void Release(Socket socket) {
            _activeSockets.Remove(socket);
            _availableSockets.Push(socket);
        }

        public void Dispose() {
            while (_availableSockets.Count > 0) {
                var socket = _availableSockets.Pop();
                socket.Close();
            }

            foreach (var socket in _activeSockets) {
                socket.Close();
            }
            _activeSockets.Clear();
        }
    }
}
