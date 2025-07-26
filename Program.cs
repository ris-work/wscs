// Program.cs
// NOT ONLY AI GENERATED CODE, contains manual code, Copyrights (c) RISHIKESHAN SULOCHANA/LAVAKUMAR
// OPEN SOFTWARE LICENSE, VERSION 3.0 ONLY (no later)

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

// Top‐level program

if (args.Length == 0)
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  --unix-listen=<path or tcp://host:port>   path or TCP endpoint to listen on");
    Console.WriteLine("  --unix-target=<path or tcp://host:port>   path or TCP endpoint to connect to");
    Console.WriteLine("  --source-type=<ws|tcp>                    which protocol to expect on source");
    Console.WriteLine("  --no-daemonize                            run in foreground");
    return;
}

var exe        = Path.GetFileNameWithoutExtension(Process.GetCurrentProcess().MainModule!.FileName);
var ipcSock    = $"{exe}.ipc.sock";
var debugSock  = $"{exe}.debug.sock";
var statsSock  = $"{exe}.stats.sock";
var inactivity = int.Parse(Environment.GetEnvironmentVariable("INACTIVITY_TIMEOUT") ?? "120000");
var noDaemon   = args.Contains("--no-daemonize");

Helpers.Verbose = noDaemon;

Console.WriteLine($"PWD: {Directory.GetCurrentDirectory()}");
Console.WriteLine($"Sockets: {ipcSock}, {debugSock}, {statsSock}");
Console.WriteLine($"No-daemonize: {noDaemon}");

var firstInstance = ShouldBeDaemon(ipcSock);
Console.WriteLine($"First-instance: {firstInstance}");

if (firstInstance)
{
    var (listen, target, stype) = ParseArgs(args);

    if (!noDaemon && !RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
    {
        Console.WriteLine("Warning: daemon mode not supported on Windows; running in foreground");
        Helpers.Verbose = true;
    }

    if (noDaemon)
    {
        Console.WriteLine("Running in foreground");
    }
    else
    {
        Console.WriteLine("Daemonizing");
        Daemonize(ipcSock, debugSock, statsSock);
    }

    if (!string.IsNullOrEmpty(listen) && !string.IsNullOrEmpty(target))
    {
        var destType = stype.Equals("ws", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
        Helpers.Log($"Initial session: {stype}:{listen} → {destType}:{target}");
        var init = new Session(listen, target, stype, destType, inactivity);
        Helpers.Sessions.Add(init);
        _ = init.RunProxy();
    }

    CleanupFiles(ipcSock, debugSock, statsSock);
    _ = IpcServer(ipcSock, inactivity);
    _ = DebugServer(debugSock);
    _ = StatsServer(statsSock);
    await Task.Delay(Timeout.Infinite);
}
else
{
    var (listen, target, stype) = ParseArgs(args);
    Helpers.Log($"Client mode: send to IPC `{ipcSock}`, msg {stype}:{listen}→{target}");
    var cmd = new IpcMsg { source = listen, dest = target, sourceType = stype };
    var msg = JsonSerializer.Serialize(cmd, JsonContext.Default.IpcMsg);

    using var cli = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    Helpers.Log($"Connecting to IPC socket `{ipcSock}`");
    await cli.ConnectAsync(new UnixDomainSocketEndPoint(ipcSock));
    Helpers.Log($"Sending IPC message: {msg}");
    await cli.SendAsync(Encoding.UTF8.GetBytes(msg), SocketFlags.None);
}

// Local static functions

static bool ShouldBeDaemon(string ipcSock)
{
    if (!File.Exists(ipcSock)) return true;
    try
    {
        using var s = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        s.Connect(new UnixDomainSocketEndPoint(ipcSock));
        return false;
    }
    catch
    {
        File.Delete(ipcSock);
        return true;
    }
}

static void Daemonize(string ipcSock, string debugSock, string statsSock)
{
    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return;
    if (NativeMethods.fork() > 0) Environment.Exit(0);
    NativeMethods.setsid();
    if (NativeMethods.fork() > 0) Environment.Exit(0);
    Directory.SetCurrentDirectory("/");
    foreach (var f in new[] { ipcSock, debugSock, statsSock })
        try { File.Delete(f); } catch { }
}

static void CleanupFiles(string ipcSock, string debugSock, string statsSock)
{
    AppDomain.CurrentDomain.ProcessExit += (_, _) =>
    {
        Console.WriteLine($"Exiting. CWD: {Directory.GetCurrentDirectory()}");
        Console.WriteLine($"Unlinking: {ipcSock}, {debugSock}, {statsSock}");
        foreach (var f in new[] { ipcSock, debugSock, statsSock })
            try { File.Delete(f); } catch { }
    };
}

static async Task IpcServer(string ipcSock, int inactivity)
{
    File.Delete(ipcSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(ipcSock));
    srv.Listen(5);
    Helpers.Log($"IPC server listening on `{ipcSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log("Accepted IPC connection");
        _ = HandleIpc(c, inactivity);
    }
}

static async Task HandleIpc(Socket c, int inactivity)
{
    var buf  = new byte[4096];
    var n    = await c.ReceiveAsync(buf, SocketFlags.None);
    var json = Encoding.UTF8.GetString(buf, 0, n);
    Helpers.Log($"IPC request JSON: {json}");
    var msg      = JsonSerializer.Deserialize<IpcMsg>(json, JsonContext.Default.IpcMsg)!;
    var destType = msg.sourceType.Equals("ws", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
    var s        = new Session(msg.source, msg.dest, msg.sourceType, destType, inactivity);
    Helpers.Sessions.Add(s);
    Helpers.Log($"New session {s.id}: {s.sourceType}:{s.source} → {s.destType}:{s.dest}");
    _ = s.RunProxy();
    c.Close();
}

static async Task DebugServer(string debugSock)
{
    File.Delete(debugSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(debugSock));
    srv.Listen(5);
    Helpers.Log($"Debug socket listening on `{debugSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log("Debug client connected");
        _ = PumpLogs(c);
    }
}

static async Task PumpLogs(Socket cli)
{
    foreach (var l in Helpers.Logs)
        await cli.SendAsync(Encoding.UTF8.GetBytes(l + "\n"), SocketFlags.None);

    var idx = Helpers.Logs.Count;
    while (true)
    {
        if (Helpers.Logs.Count > idx)
        {
            var msg = Helpers.Logs.ElementAt(idx++);
            await cli.SendAsync(Encoding.UTF8.GetBytes(msg + "\n"), SocketFlags.None);
        }
        await Task.Delay(100);
    }
}

static async Task StatsServer(string statsSock)
{
    File.Delete(statsSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(statsSock));
    srv.Listen(1);
    Helpers.Log($"Stats socket listening on `{statsSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log("Stats client connected");
        var arr = Helpers.Sessions
            .Select(s => new SessionStats {
                id         = s.id,
                source     = s.source,
                dest       = s.dest,
                sourceType = s.sourceType,
                destType   = s.destType,
                alive      = s.IsAlive
            })
            .ToArray();

        var js = JsonSerializer.Serialize(arr, JsonContext.Default.SessionStatsArray);
        await c.SendAsync(Encoding.UTF8.GetBytes(js), SocketFlags.None);
        c.Close();
    }
}

static (string listen, string target, string stype) ParseArgs(string[] args)
{
    string? a     = null;
    string? b     = null;
    string  ctype = "ws";

    foreach (var arg in args)
    {
        if (arg.StartsWith("--unix-listen="))  a     = arg.Split('=', 2)[1];
        if (arg.StartsWith("--unix-target="))  b     = arg.Split('=', 2)[1];
        if (arg.StartsWith("--source-type="))  ctype = arg.Split('=', 2)[1];
    }

    return (a ?? string.Empty, b ?? string.Empty, ctype);
}

static class NativeMethods
{
    [DllImport("libc")]
    public static extern int fork();

    [DllImport("libc")]
    public static extern int setsid();
}

// Helpers, DTOs, Session, WebSocket handshake and JSON context

static class Helpers
{
    public static bool Verbose;
    public static readonly ConcurrentBag<Session> Sessions = new();
    public static readonly ConcurrentBag<string>  Logs     = new();

    public static void Log(string message)
    {
        var entry = $"{DateTime.Now:HH:mm:ss} {message}";
        Logs.Add(entry);
        if (Verbose)
            Console.WriteLine(entry);
    }
}

class Session
{
    static int seq;
    public int    id;
    public string source, dest;
    public string sourceType, destType;
    public DateTime last;
    public int    timeout;
    public bool   IsAlive => (DateTime.Now - last).TotalMilliseconds < timeout;

    public Session(string s, string d, string st, string dt, int to)
    {
        id         = Interlocked.Increment(ref seq);
        source     = s;
        dest       = d;
        sourceType = st;
        destType   = dt;
        timeout    = to;
        last       = DateTime.Now;
    }

    public async Task RunProxy()
    {
        if (sourceType.Equals("ws", StringComparison.OrdinalIgnoreCase))
            await RunWsToTcp();
        else
            await RunTcpToWs();
    }

    async Task RunWsToTcp()
    {
        Helpers.Log($"Session {id} WS→TCP: listen `{source}`, connect `{dest}`");
        var srv = BuildListener(source);
        srv.Listen(1);

        while (true)
        {
            var sock = await srv.AcceptAsync();
            Helpers.Log($"Session {id} – WS handshake incoming");
            var ns = new NetworkStream(sock, ownsSocket: true);
            var ws = await UnixWS.AcceptWebSocketAsync(ns, CancellationToken.None);

            Helpers.Log($"Session {id} – WS linked, dialing TCP `{dest}`");
            var tcp = BuildConnector(dest);
            await ConnectSocketAsync(tcp, dest);
            var ts  = new NetworkStream(tcp, ownsSocket: true);

            var t1 = Pipe(ws, ts);
            var t2 = Pipe(ts, ws);
            await Task.WhenAll(t1, t2);
            Helpers.Log($"Session {id} – WS→TCP proxy ended");
        }
    }

    async Task RunTcpToWs()
    {
        Helpers.Log($"Session {id} TCP→WS: listen `{source}`, WS‐dial `{dest}`");
        var srv = BuildListener(source);
        srv.Listen(1);

        while (true)
        {
            var sock = await srv.AcceptAsync();
            var ns   = new NetworkStream(sock, ownsSocket: true);

            Helpers.Log($"Session {id} – dialing WS `{dest}`");
            var ws   = await ConnectWebSocket(dest);

            var t1 = Pipe(ns, ws);
            var t2 = Pipe(ws, ns);
            await Task.WhenAll(t1, t2);
            Helpers.Log($"Session {id} – TCP→WS proxy ended");
        }
    }

    async Task Pipe(WebSocket ws, Stream stm)
    {
        var buf = new byte[8192];
        var seg = new ArraySegment<byte>(buf);
        while (ws.State == WebSocketState.Open)
        {
            var r = await ws.ReceiveAsync(seg, CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) break;
            last = DateTime.Now;
            Helpers.Log(
                $"Session {id} pumped {r.Count} bytes WS→TCP " +
                $"({sourceType}:{source}→{destType}:{dest})"
            );
            await stm.WriteAsync(buf, 0, r.Count);
        }
    }

    async Task Pipe(Stream stm, WebSocket ws)
    {
        var buf = ArrayPool<byte>.Shared.Rent(8192);
        try
        {
            while (true)
            {
                var n = await stm.ReadAsync(buf, 0, buf.Length);
                if (n == 0) break;
                last = DateTime.Now;
                Helpers.Log(
                    $"Session {id} pumped {n} bytes TCP→WS " +
                    $"({sourceType}:{source}→{destType}:{dest})"
                );
                await ws.SendAsync(
                    new ArraySegment<byte>(buf, 0, n),
                    WebSocketMessageType.Binary,
                    true,
                    CancellationToken.None
                );
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    async Task ConnectSocketAsync(Socket sock, string endpoint)
    {
        if (endpoint.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            var (host, port) = ParseTcp(endpoint);
            await sock.ConnectAsync(
                new IPEndPoint(IPAddress.Parse(host), port),
                CancellationToken.None
            );
        }
        else
        {
            await sock.ConnectAsync(
                new UnixDomainSocketEndPoint(endpoint),
                CancellationToken.None
            );
        }
    }

    Socket BuildListener(string endpoint)
    {
        if (endpoint.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            var (host, port) = ParseTcp(endpoint);
            var ip   = IPAddress.Parse(host);
            var sock = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            sock.Bind(new IPEndPoint(ip, port));
            return sock;
        }
        else
        {
            File.Delete(endpoint);
            var sock = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            sock.Bind(new UnixDomainSocketEndPoint(endpoint));
            return sock;
        }
    }

    Socket BuildConnector(string endpoint)
    {
        if (endpoint.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            var (host, port) = ParseTcp(endpoint);
            return new Socket(
                IPAddress.Parse(host).AddressFamily,
                SocketType.Stream,
                ProtocolType.Tcp
            );
        }
        else
        {
            return new Socket(
                AddressFamily.Unix,
                SocketType.Stream,
                ProtocolType.Unspecified
            );
        }
    }

    (string host, int port) ParseTcp(string endpoint)
    {
        var without = endpoint["tcp://".Length..];
        var idx     = without.LastIndexOf(':');
        var host    = without[..idx];
        var port    = int.Parse(without[(idx + 1)..]);
        return (host, port);
    }

    async Task<WebSocket> ConnectWebSocket(string dest)
    {
        if (dest.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            var (host, port) = ParseTcp(dest);
            var ws = new ClientWebSocket();
            await ws.ConnectAsync(new Uri($"ws://{host}:{port}/"), CancellationToken.None);
            return ws;
        }
        else
        {
            return await UnixWS.ConnectAsync(
                socketPath:        dest,
                host:              "localhost",
                resource:          "/",
                subProtocol:       null,
                cancellationToken: CancellationToken.None
            );
        }
    }
}

public static class UnixWS
{
    const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    public static async Task<WebSocket> AcceptWebSocketAsync(
        NetworkStream stream,
        CancellationToken cancellationToken)
    {
        var hdr = new byte[16_384];
        var tot = 0;
        while (true)
        {
            var n = await stream.ReadAsync(hdr, tot, hdr.Length - tot, cancellationToken);
            if (n == 0) throw new Exception("Handshake aborted");
            tot += n;
            if (Encoding.ASCII.GetString(hdr, 0, tot).Contains("\r\n\r\n"))
                break;
        }

        var header = Encoding.ASCII.GetString(hdr, 0, tot);
        var key    = header
            .Split("\r\n", StringSplitOptions.RemoveEmptyEntries)
            .FirstOrDefault(l =>
                l.StartsWith("Sec-WebSocket-Key:", StringComparison.OrdinalIgnoreCase))
            ?.Split(":", 2)[1].Trim()
            ?? throw new Exception("Missing WS key");

        var accept = ComputeAccept(key);
        var resp   =
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Accept: {accept}\r\n\r\n";

        await stream.WriteAsync(Encoding.ASCII.GetBytes(resp), cancellationToken);

        return WebSocket.CreateFromStream(
            stream,
            isServer: true,
            subProtocol: null,
            keepAliveInterval: TimeSpan.FromMinutes(2)
        );
    }

    public static async Task<WebSocket> ConnectAsync(
        string socketPath,
        string host,
        string resource,
        string subProtocol,
        CancellationToken cancellationToken)
    {
        var sock = new Socket(
            AddressFamily.Unix,
            SocketType.Stream,
            ProtocolType.Unspecified
        );

        await sock.ConnectAsync(
            new UnixDomainSocketEndPoint(socketPath),
            cancellationToken
        );

        var stream = new NetworkStream(sock, ownsSocket: true);

        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var req = new[]
        {
            $"GET {resource} HTTP/1.1",
            $"Host: {host}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            $"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            subProtocol != null ? $"Sec-WebSocket-Protocol: {subProtocol}" : null,
            "", ""
        }
        .Where(x => x != null)
        .Aggregate((a, b) => a + "\r\n" + b);

        var reqBytes = Encoding.ASCII.GetBytes(req);
        await stream.WriteAsync(reqBytes, 0, reqBytes.Length, cancellationToken);

        var buf = new byte[1024];
        var n   = await stream.ReadAsync(buf, 0, buf.Length, cancellationToken);
        var resp = Encoding.ASCII.GetString(buf, 0, n);
        if (!resp.Contains("101 Switching Protocols"))
            throw new Exception("WebSocket handshake failed: " + resp);

        return WebSocket.CreateFromStream(
            stream,
            isServer: false,
            subProtocol,
            TimeSpan.FromMinutes(2)
        );
    }

    static string ComputeAccept(string key)
    {
        using var sha1 = SHA1.Create();
        var hash = sha1.ComputeHash(
            Encoding.ASCII.GetBytes(key + WebSocketGuid)
        );
        return Convert.ToBase64String(hash);
    }
}

class IpcMsg
{
    public string source     { get; set; } = "";
    public string dest       { get; set; } = "";
    public string sourceType { get; set; } = "";
}

class SessionStats
{
    public int    id         { get; set; }
    public string source     { get; set; } = "";
    public string dest       { get; set; } = "";
    public string sourceType { get; set; } = "";
    public string destType   { get; set; } = "";
    public bool   alive      { get; set; }
}

static class Extensions
{
    public static T Also<T>(this T obj, Action<T> act) { act(obj); return obj; }
}

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(IpcMsg))]
[JsonSerializable(typeof(SessionStats[]))]
internal partial class JsonContext : JsonSerializerContext { }

