using System;
using System.Buffers;
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
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;

// top‐level startup

var exe        = Path.GetFileNameWithoutExtension(Process.GetCurrentProcess().MainModule!.FileName);
var ipcSock    = $"{exe}.ipc.sock";
var debugSock  = $"{exe}.debug.sock";
var statsSock  = $"{exe}.stats.sock";
var inactivity = int.Parse(Environment.GetEnvironmentVariable("INACTIVITY_TIMEOUT") ?? "120000");

if (ShouldBeDaemon())
{
    Daemonize();
    CleanupFiles();
    _ = IpcServer();
    _ = DebugServer();
    _ = StatsServer();
    await Task.Delay(Timeout.Infinite);
}
else
{
    var (listen, target, stype) = ParseArgs();
    var cmd = new IpcMsg { source = listen, dest = target, sourceType = stype };
    var msg = JsonSerializer.Serialize(cmd, JsonContext.Default.IpcMsg);
    using var cli = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    cli.Connect(new UnixDomainSocketEndPoint(ipcSock));
    await cli.SendAsync(Encoding.UTF8.GetBytes(msg), SocketFlags.None);
}

// local functions

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

void Daemonize()
{
    if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return;
    if (fork() > 0) Environment.Exit(0);
    setsid();
    if (fork() > 0) Environment.Exit(0);
    Directory.SetCurrentDirectory("/");
    foreach (var f in new[] { ipcSock, debugSock, statsSock })
        File.Delete(f);
}

[DllImport("libc")] static extern int fork();
[DllImport("libc")] static extern int setsid();

void CleanupFiles()
{
    AppDomain.CurrentDomain.ProcessExit += (_, _) =>
    {
        Console.WriteLine($"Exiting. CWD: {Directory.GetCurrentDirectory()}");
        Console.WriteLine($"Unlinking sockets: {ipcSock}, {debugSock}, {statsSock}");
        foreach (var f in new[] { ipcSock, debugSock, statsSock })
            try { File.Delete(f); } catch { }
    };
}

async Task IpcServer()
{
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(ipcSock));
    srv.Listen(5);

    while (true)
    {
        var c = await srv.AcceptAsync();
        _ = HandleIpc(c);
    }
}

async Task HandleIpc(Socket c)
{
    var buf  = new byte[4096];
    var n    = await c.ReceiveAsync(buf, SocketFlags.None);
    var json = Encoding.UTF8.GetString(buf, 0, n);
    var msg  = JsonSerializer.Deserialize(json, JsonContext.Default.IpcMsg)!;
    var destType = msg.sourceType.Equals("ws", StringComparison.OrdinalIgnoreCase) ? "tcp" : "ws";
    var s = new Session(msg.source, msg.dest, msg.sourceType, destType, inactivity);
    Helpers.Sessions.Add(s);
    Helpers.Log($"new session {s.id} {s.sourceType}->{s.destType}");
    _ = s.RunProxy();
    c.Close();
}

async Task DebugServer()
{
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(debugSock));
    srv.Listen(5);

    while (true)
    {
        var c = await srv.AcceptAsync();
        _ = PumpLogs(c);
    }
}

async Task PumpLogs(Socket cli)
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

async Task StatsServer()
{
    var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    srv.Bind(new UnixDomainSocketEndPoint(statsSock));
    srv.Listen(1);

    while (true)
    {
        var c = await srv.AcceptAsync();
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
        var js  = JsonSerializer.Serialize(arr, JsonContext.Default.SessionStatsArray);
        await c.SendAsync(Encoding.UTF8.GetBytes(js), SocketFlags.None);
        c.Close();
    }
}

(string listen, string target, string stype) ParseArgs()
{
    string? a = null, b = null, c = "ws";
    foreach (var arg in Environment.GetCommandLineArgs())
    {
        if (arg.StartsWith("--unix-listen="))  a = arg.Split('=', 2)[1];
        if (arg.StartsWith("--unix-target="))  b = arg.Split('=', 2)[1];
        if (arg.StartsWith("--source-type="))  c = arg.Split('=', 2)[1];
    }
    return (a!, b!, c!);
}

// helpers & DTOs

static class Helpers
{
    public static readonly ConcurrentBag<Session> Sessions = new();
    public static readonly ConcurrentBag<string> Logs = new();
    public static void Log(string m)
    {
        var t = $"{DateTime.Now:HH:mm:ss} {m}";
        Logs.Add(t);
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

class Session
{
    static int seq;
    public int id                    = Interlocked.Increment(ref seq);
    public string source, dest;
    public string sourceType, destType;
    public DateTime last             = DateTime.Now;
    public int timeout;
    public bool IsAlive             => (DateTime.Now - last).TotalMilliseconds < timeout;

    public Session(string s, string d, string st, string dt, int to)
    {
        source     = s;
        dest       = d;
        sourceType = st;
        destType   = dt;
        timeout    = to;
    }

    public async Task RunProxy()
    {
        try
        {
            if (sourceType == "ws") await RunWsToTcp();
            else                   await RunTcpToWs();
        }
        catch (Exception e)
        {
            Helpers.Log($"session {id} error: {e.Message}");
        }
    }

    async Task RunWsToTcp()
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureWebHostDefaults(web => web
                .UseKestrel(opts => opts.ListenUnixSocket(source))
                .Configure(app => app.UseWebSockets()
                    .Run(async ctx =>
                    {
                        var ws  = await ctx.WebSockets.AcceptWebSocketAsync();
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
        var srv = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        srv.Bind(new UnixDomainSocketEndPoint(source));
        srv.Listen(1);

        while (true)
        {
            var c = await srv.AcceptAsync();
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
        while (ws.State == WebSocketState.Open)
        {
            var r = await ws.ReceiveAsync(seg, CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) break;
            last = DateTime.Now;
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
                await ws.SendAsync(new ArraySegment<byte>(buf, 0, n),
                                   WebSocketMessageType.Binary, true, CancellationToken.None);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }
}

// JSON source‐generation context for AoT

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(IpcMsg))]
[JsonSerializable(typeof(SessionStats[]))]
internal partial class JsonContext : JsonSerializerContext { }

