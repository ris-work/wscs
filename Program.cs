// Program.cs
using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;

// compute names and flags
var exe        = Path.GetFileNameWithoutExtension(Process.GetCurrentProcess().MainModule!.FileName);
var ipcSock    = $"{exe}.ipc.sock";
var debugSock  = $"{exe}.debug.sock";
var statsSock  = $"{exe}.stats.sock";
var inactivity = int.Parse(Environment.GetEnvironmentVariable("INACTIVITY_TIMEOUT") ?? "120000");
var noDaemon   = args.Contains("--no-daemonize");

// verbose logging if foreground
Helpers.Verbose = noDaemon;

// initial info
Console.WriteLine($"PWD: {Directory.GetCurrentDirectory()}");
Console.WriteLine($"Sockets: {ipcSock}, {debugSock}, {statsSock}");
Console.WriteLine($"No-daemonize: {noDaemon}");

// decide mode
var firstInstance = ShouldBeDaemon();
Console.WriteLine($"First-instance: {firstInstance}");

if (firstInstance)
{
    var (listen, target, stype) = ParseArgs();

    if (noDaemon)
        Console.WriteLine("Running in foreground");
    else
    {
        Console.WriteLine("Daemonizing");
        Daemonize();
    }

    // if user provided an initial session on the command line, spin it up
    if (!string.IsNullOrEmpty(listen) && !string.IsNullOrEmpty(target))
    {
        var destType = stype.Equals("ws", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
        Helpers.Log($"Initial session: {stype}:{listen} → {destType}:{target}");
        var init = new Session(listen, target, stype, destType, inactivity);
        Helpers.Sessions.Add(init);
        _ = init.RunProxy();
    }

    CleanupFiles();
    _ = IpcServer();
    _ = DebugServer();
    _ = StatsServer();
    await Task.Delay(Timeout.Infinite);
}
else
{
    var (listen, target, stype) = ParseArgs();
    Helpers.Log($"Client mode: send to ipc `{ipcSock}`, msg {stype}:{listen}→{target}");
    var cmd = new IpcMsg { source = listen, dest = target, sourceType = stype };
    var msg = JsonSerializer.Serialize(cmd, JsonContext.Default.IpcMsg);

    using var cli = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    Helpers.Log($"Connecting to IPC socket `{ipcSock}`");
    await cli.ConnectAsync(new UnixDomainSocketEndPoint(ipcSock));
    Helpers.Log($"Sending IPC message: {msg}");
    await cli.SendAsync(Encoding.UTF8.GetBytes(msg), SocketFlags.None);
}

// determine first instance by testing ipc socket
bool ShouldBeDaemon()
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

// fork/detach on Linux
void Daemonize()
{
    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return;
    if (fork() > 0) Environment.Exit(0);
    setsid();
    if (fork() > 0) Environment.Exit(0);
    Directory.SetCurrentDirectory("/");
    // unlink any stray sockets
    foreach (var f in new[] { ipcSock, debugSock, statsSock })
        try { File.Delete(f); } catch { }
}

[DllImport("libc")] static extern int fork();
[DllImport("libc")] static extern int setsid();

// cleanup on exit
void CleanupFiles()
{
    AppDomain.CurrentDomain.ProcessExit += (_, _) =>
    {
        Console.WriteLine($"Exiting. CWD: {Directory.GetCurrentDirectory()}");
        Console.WriteLine($"Unlinking: {ipcSock}, {debugSock}, {statsSock}");
        foreach (var f in new[] { ipcSock, debugSock, statsSock })
            try { File.Delete(f); } catch { }
    };
}

// IPC server: accept JSON commands
async Task IpcServer()
{
    File.Delete(ipcSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(ipcSock));
    srv.Listen(5);
    Helpers.Log($"IPC server listening on `{ipcSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log($"Accepted IPC connection");
        _ = HandleIpc(c);
    }
}

async Task HandleIpc(Socket c)
{
    var buf  = new byte[4096];
    var n    = await c.ReceiveAsync(buf, SocketFlags.None);
    var json = Encoding.UTF8.GetString(buf, 0, n);
    Helpers.Log($"IPC request JSON: {json}");
    var msg  = JsonSerializer.Deserialize(json, JsonContext.Default.IpcMsg)!;
    var destType = msg.sourceType.Equals("ws", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
    var s = new Session(msg.source, msg.dest, msg.sourceType, destType, inactivity);
    Helpers.Sessions.Add(s);
    Helpers.Log($"New session {s.id}: {s.sourceType}:{s.source} → {s.destType}:{s.dest}");
    _ = s.RunProxy();
    c.Close();
}

// debug socket: stream logs to clients
async Task DebugServer()
{
    File.Delete(debugSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(debugSock));
    srv.Listen(5);
    Helpers.Log($"Debug socket listening on `{debugSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log($"Debug client connected");
        _ = PumpLogs(c);
    }
}

async Task PumpLogs(Socket cli)
{
    // send backlog
    foreach (var l in Helpers.Logs)
        await cli.SendAsync(Encoding.UTF8.GetBytes(l + "\n"), SocketFlags.None);

    // tail
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

// stats socket: dump sessions once per connect
async Task StatsServer()
{
    File.Delete(statsSock);
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(statsSock));
    srv.Listen(1);
    Helpers.Log($"Stats socket listening on `{statsSock}`");

    while (true)
    {
        var c = await srv.AcceptAsync();
        Helpers.Log($"Stats client connected");
        var arr = Helpers.Sessions
            .Select(s => new SessionStats {
                id = s.id,
                source = s.source,
                dest = s.dest,
                sourceType = s.sourceType,
                destType = s.destType,
                alive = s.IsAlive
            })
            .ToArray();

        var js = JsonSerializer.Serialize(arr, JsonContext.Default.SessionStatsArray);
        await c.SendAsync(Encoding.UTF8.GetBytes(js), SocketFlags.None);
        c.Close();
    }
}

// parse client args
(string listen, string target, string stype) ParseArgs()
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

// logging and sessions
static class Helpers
{
    public static bool Verbose;
    public static readonly ConcurrentBag<Session> Sessions = new();
    public static readonly ConcurrentBag<string>  Logs     = new();

    public static void Log(string m)
    {
        var t = $"{DateTime.Now:HH:mm:ss} {m}";
        Logs.Add(t);
        if (Verbose)
            Console.WriteLine(t);
    }
}

// DTOs
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

// proxy session
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
        try
        {
            if (sourceType == "ws")
                await RunWsToTcp();
            else
                await RunTcpToWs();
        }
        catch (Exception e)
        {
            Helpers.Log($"Session {id} error: {e.Message}");
        }
    }

    async Task RunWsToTcp()
    {
        Helpers.Log($"Session {id} – WS→TCP: listen `{source}`, connect `{dest}`");
        var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(web => web
                .UseKestrel(opts => opts.ListenUnixSocket(source))
                .Configure(app => app.UseWebSockets()
                    .Run(async ctx =>
                    {
                        var ws = await ctx.WebSockets.AcceptWebSocketAsync();
                        Helpers.Log($"Session {id} – WS accepted, TCP connect `{dest}`");
                        using var tcp = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                        tcp.Connect(new UnixDomainSocketEndPoint(dest));
                        using var ns = new NetworkStream(tcp, true);
                        await Task.WhenAll(Pipe(ws, ns), Pipe(ns, ws));
                    })))
            .Build();

        await host.RunAsync();
    }

    async Task RunTcpToWs()
    {
        Helpers.Log($"Session {id} – TCP→WS: listen `{source}`, connect `{dest}`");
        var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        srv.Bind(new UnixDomainSocketEndPoint(source));
        srv.Listen(1);

        while (true)
        {
            var c = await srv.AcceptAsync();
            Helpers.Log($"Session {id} – TCP accepted, WS→`{dest}`");
            using var ns = new NetworkStream(c, true);
            var ws = new ClientWebSocket();
            await ws.ConnectAsync(new Uri($"ws+unix://{dest}"), CancellationToken.None);
            await Task.WhenAll(Pipe(ns, ws), Pipe(ws, ns));
        }
    }

    async Task Pipe(WebSocket ws, Stream stm)
    {
        var buf = new byte[8192];
        var seg = new ArraySegment<byte>(buf);

        Helpers.Log($"Session {id} – start WS→TCP forwarding");
        while (ws.State == WebSocketState.Open)
        {
            var r = await ws.ReceiveAsync(seg, CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) break;
            last = DateTime.Now;
            await stm.WriteAsync(buf, 0, r.Count);
        }
        Helpers.Log($"Session {id} – WS→TCP forwarding ended");
    }

    async Task Pipe(Stream stm, WebSocket ws)
    {
        var buf = ArrayPool<byte>.Shared.Rent(8192);

        try
        {
            Helpers.Log($"Session {id} – start TCP→WS forwarding");
            while (true)
            {
                var n = await stm.ReadAsync(buf, 0, buf.Length);
                if (n == 0) break;
                last = DateTime.Now;
                await ws.SendAsync(new ArraySegment<byte>(buf, 0, n),
                                   WebSocketMessageType.Binary, true, CancellationToken.None);
            }
            Helpers.Log($"Session {id} – TCP→WS forwarding ended");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }
}

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(IpcMsg))]
[JsonSerializable(typeof(SessionStats[]))]
internal partial class JsonContext : JsonSerializerContext { }

