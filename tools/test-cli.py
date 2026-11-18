"""CLI lifecycle regression, no network or third-party Python packages required."""
import hashlib, http.server, pathlib, subprocess, tempfile, threading, time, sys, sqlite3
EXE = pathlib.Path(sys.argv[1] if len(sys.argv)>1 else 'target/release/rd-cli.exe').resolve()
DATA = bytes(range(251)) * 20000
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def serve(self, body):
        start, end = 0, len(DATA)-1
        value=self.headers.get('Range')
        if value:
            start, raw_end=value.removeprefix('bytes=').split('-')
            start=int(start); end=int(raw_end) if raw_end else end
        self.send_response(206 if value else 200)
        self.send_header('Content-Length', str(end-start+1))
        self.send_header('Accept-Ranges','bytes'); self.send_header('ETag','"cli-test"')
        if value: self.send_header('Content-Range',f'bytes {start}-{end}/{len(DATA)}')
        self.end_headers()
        if body:
            try:
                for offset in range(start,end+1,32768):
                    self.wfile.write(DATA[offset:min(offset+32768,end+1)]); self.wfile.flush(); time.sleep(.002)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError): pass
    def do_HEAD(self): self.serve(False)
    def do_GET(self): self.serve(True)
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory() as folder:
    root=pathlib.Path(folder); db=root/'test.db'; out=root/'downloads'
    def run(*args, expected=0):
        r=subprocess.run([str(EXE),*args,'--db',str(db),'--direct'],cwd=root,capture_output=True,text=True,timeout=40)
        assert r.returncode==expected,(args,r.returncode,r.stdout,r.stderr)
        return r.stdout
    url=f'http://127.0.0.1:{server.server_port}/fixture.bin'
    digest=hashlib.sha256(DATA).hexdigest()
    first=run(url,'-o',str(out),'--sha256',digest)
    assert 'completed:' in first and (out/'fixture.bin').read_bytes()==DATA
    assert 'already completed' in run(url,'-o',str(out),'--sha256',digest)
    listing=run('list'); assert listing.count('#')==1,listing
    assert 'completed' in run('status','1')
    run(url,'-o',str(out),'--new')
    assert run('list').count('#')==2
    run('remove','2','--delete-file')
    assert run('list').count('#')==1
    # Interrupt a process with accepted data, then recover the SAME task.
    process=subprocess.Popen([str(EXE),url,'-n','1','--limit','1048576','--new',
        '--db',str(db),'--direct','-o',str(out),'--sha256',digest],cwd=root,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        time.sleep(1.5)
        assert process.poll() is None
        run('list',expected=1) # A second engine must not own the same part files.
    finally:
        process.kill(); process.wait(timeout=10)
    # Simulate the inflated legacy task counter; segment checkpoints remain
    # valid. Recovery must reset the counter and report new transferred bytes.
    with sqlite3.connect(db) as conn:
        conn.execute('UPDATE tasks SET downloaded = 9999999999 WHERE id = 3')
    conn.close()
    resumed=run(url,'-n','1','--limit','0','-o',str(out),'--sha256',digest)
    assert 'resuming task #3' in resumed, resumed
    assert 'transferred this run: 0.00 B' not in resumed, resumed
    assert 'sha256: '+digest in resumed, resumed
    assert run('list').count('#')==2
    run('remove','3','--delete-file')
    run('list','--config','missing.toml') # Optional absent config uses defaults.
    bad=root/'bad.toml'; bad.write_text('this is not TOML')
    run('list','--config',str(bad),expected=2)
    run(url,'-n','garbage',expected=2)
    run('remove','1','--delete-file')
    assert not (out/'fixture.bin').exists()
server.shutdown()
print('CLI lifecycle, checksums, deduplication, crash/resume, exclusive ownership, removal and invalid-input tests passed')
