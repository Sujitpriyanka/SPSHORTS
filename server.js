'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const dir = path.resolve(process.env.SP_SHORTS_DATA || 'sp-shorts-data');
const media = path.join(dir,'media');
fs.mkdirSync(media,{recursive:true});
const dbPath = path.join(dir,'data.json');
const db = fs.existsSync(dbPath) ? JSON.parse(fs.readFileSync(dbPath,'utf8')) : { people:{}, videos:{} };
const maxVideo = 250 * 1024 * 1024;
const liveUploads = new Set();
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const save = () => {
    fs.writeFileSync(dbPath + '.tmp',JSON.stringify(db),{mode:0o600});
    fs.renameSync(dbPath + '.tmp',dbPath);
};
const error = (status,message) => Object.assign(Error(message),{status});
const reply = (res,status,data) => {
    res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
    res.end(JSON.stringify(data));
};
async function readProfile(req) {
    const parts = [];
    let size = 0;
    for await (const part of req) {
        size += part.length;
        if (size > 256 * 1024) throw error(413,'Profile is too large');
        parts.push(part);
    }
    let data;
    try { data = JSON.parse(Buffer.concat(parts).toString('utf8')); }
    catch { throw error(400,'Invalid profile data'); }
    const user = String(data.user || '').trim().toLowerCase();
    const name = String(data.name || '').trim().slice(0,35);
    if (!/^[a-z0-9_.]{2,24}$/.test(user) || !name) throw error(400,'Use 2–24 letters, numbers, dots or underscores for your username.');
    const avatar = String(data.avatar || '');
    if (avatar && !/^data:image\/jpeg;base64,[A-Za-z0-9+/]+=*$/.test(avatar)) throw error(400,'Invalid profile photo');
    return { user,name,bio:String(data.bio || '').slice(0,100),avatar };
}
const users = () => Object.values(db.people);
const publicUser = ({tokenHash,...person}) => person;
function authenticated(req) {
    const token = (req.headers.authorization || '').replace(/^Bearer /,'');
    if (!/^[a-f0-9]{64}$/.test(token)) throw error(401,'Connect your account first.');
    const hash = digest(token);
    const person = users().find(candidate => candidate.tokenHash === hash);
    if (!person) throw error(401,'This server does not recognise your account.');
    return person;
}
function nameAvailable(user,id) {
    if (users().some(person => person.user === user && person.id !== id)) {
        throw error(409,'That username is already taken. Choose another in Edit Profile.');
    }
}
async function upload(req,res,url) {
    const person = authenticated(req);
    const key = url.searchParams.get('key') || '';
    const title = (url.searchParams.get('title') || '').trim().slice(0,140);
    const ext = url.searchParams.get('ext');
    if (!/^[a-zA-Z0-9_-]{1,64}$/.test(key) || !title || !['mp4','webm'].includes(ext)) throw error(400,'Invalid upload details');
    const id = digest(person.id + ':' + key).slice(0,32);
    if (db.videos[id]) { req.resume(); return reply(res,200,db.videos[id]); }
    if (liveUploads.has(id)) throw error(409,'This video is already uploading.');
    if (Number(req.headers['content-length']) > maxVideo) throw error(413,'Video limit is 250 MB.');
    const filename = id + '.' + ext;
    const temporary = path.join(media,filename + '.tmp');
    let size = 0;
    let signature = Buffer.alloc(0);
    liveUploads.add(id);
    try {
        const limiter = new Transform({transform(chunk,_,done) {
            size += chunk.length;
            if (signature.length < 12) signature = Buffer.concat([signature,chunk.subarray(0,12-signature.length)]);
            done(size <= maxVideo ? null : error(413,'Video limit is 250 MB.'),chunk);
        }});
        await pipeline(req,limiter,fs.createWriteStream(temporary,{mode:0o600}));
        const isMp4 = signature.length >= 12 && signature.subarray(4,8).toString('ascii') === 'ftyp';
        const isWebm = signature.length >= 4 && signature.subarray(0,4).toString('hex') === '1a45dfa3';
        if (!(ext === 'mp4' ? isMp4 : isWebm)) throw error(400,'Upload a valid MP4 or WebM video.');

        fs.renameSync(temporary,path.join(media,filename));
        const video = {id,ownerId:person.id,title,filename,created:Date.now(),key};
        db.videos[id] = video;
        try { save(); } catch (err) {
            delete db.videos[id];
            fs.rmSync(path.join(media,filename),{force:true});
            throw err;
        }
        reply(res,201,video);
    } finally {
        liveUploads.delete(id);
        fs.rmSync(temporary,{force:true});
    }
}
function play(req,res,filename) {
    const exists = db.videos[filename.split('.')[0]];
    if (!exists || exists.filename !== filename) throw error(404,'Video not found');
    const file = path.join(media,filename);
    const size = fs.statSync(file).size;
    let start=0,end=size-1,status=200;
    const range = req.headers.range;
    if (range) {
        const match = /^bytes=(\d*)-(\d*)$/.exec(range);
        if (!match || (!match[1] && !match[2])) throw error(416,'Invalid range');
        start = match[1] ? Number(match[1]) : Math.max(0,size-Number(match[2]));
        end = match[1] && match[2] ? Math.min(end,Number(match[2])) : end;
        if (start > end || start < 0) throw error(416,'Invalid range');
        status = 206;
    }
    res.writeHead(status,{'Content-Type':filename.endsWith('.webm')?'video/webm':'video/mp4',
        'Content-Length':end-start+1,'Accept-Ranges':'bytes',
        ...(status===206?{'Content-Range':`bytes ${start}-${end}/${size}`}:{})});
    if (req.method === 'HEAD') return res.end();
    fs.createReadStream(file,{start,end}).on('error',()=>res.destroy()).pipe(res);
}

const rooms = new Map();
function liveConfig() {
    const url = process.env.LIVEKIT_URL || '';
    const key = process.env.LIVEKIT_API_KEY || '';
    const secret = process.env.LIVEKIT_API_SECRET || '';
    return {url,key,secret,ready:/^wss:\/\/[a-z0-9.-]+(?::\d+)?\/?$/i.test(url) && !!key && !!secret};
}
function roomList() {
    for (const [id,room] of rooms) if (Date.now()-room.updated>90000) rooms.delete(id);
    return [...rooms.values()];
}
async function roomBody(req) {
    let body='';
    for await (const chunk of req) {
        body+=chunk.toString();
        if (Buffer.byteLength(body)>4096) throw error(413,'Room request is too large');
    }
    try { return JSON.parse(body || '{}'); } catch { throw error(400,'Invalid room request'); }
}
function roomToken(person,room) {
    const config=liveConfig();
    if (!config.ready) throw error(503,'Live audio is not configured on the server.');
    const now=Math.floor(Date.now()/1000);
    const host=room.ownerId===person.id;
    const publish=host || room.mode==='Audio';
    const encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
    const payload={iss:config.key,sub:person.id,name:person.name,nbf:now-5,exp:now+600,
        video:{room:room.id,roomJoin:true,canSubscribe:true,canPublish:publish,
            canPublishData:true,canPublishSources:host && room.mode==='Live'?['camera','microphone']:['microphone']}};
    const unsigned=encode({alg:'HS256',typ:'JWT'})+'.'+encode(payload);
    const token=unsigned+'.'+crypto.createHmac('sha256',config.secret).update(unsigned).digest('base64url');
    return {url:config.url,token,room,role:host?'host':'guest',expiresAt:(now+600)*1000};
}

const server = http.createServer(async (req,res) => {
    try {
        const url = new URL(req.url,'http://localhost');
        if (req.method==='GET' && url.pathname==='/healthz') return reply(res,200,{ok:true,service:'sp-shorts',livekitConfigured:liveConfig().ready});
        if (req.method==='GET' && url.pathname==='/api/rooms') return reply(res,200,{enabled:liveConfig().ready,rooms:roomList()});
        if (req.method==='POST' && ['/api/rooms','/api/rooms/join','/api/rooms/heartbeat','/api/rooms/leave'].includes(url.pathname)) {
            const person=authenticated(req);
            const body=await roomBody(req);
            roomList();
            if (url.pathname==='/api/rooms') {
                if (!liveConfig().ready) throw error(503,'Live audio is not configured on the server.');
                if (!['Live','Audio'].includes(body.mode)) throw error(400,'Choose Live or Audio');
                const id='sp-'+person.id;
                const room={id,ownerId:person.id,host:publicUser(person),mode:body.mode,
                    title:String(body.title || person.name+"'s room").trim().slice(0,80),updated:Date.now()};
                rooms.set(id,room);
                return reply(res,201,roomToken(person,room));
            }
            const room=rooms.get(String(body.roomId || ''));
            if (!room) throw error(404,'This live room has ended.');
            if (url.pathname==='/api/rooms/join') return reply(res,200,roomToken(person,room));
            if (room.ownerId!==person.id) throw error(403,'Only the host can update this room.');
            if (url.pathname==='/api/rooms/leave') rooms.delete(room.id);
            else room.updated=Date.now();
            return reply(res,200,{ok:true});
        }
        if (req.method==='GET' && url.pathname==='/api/catalog') return reply(res,200,{
            users:users().map(publicUser),videos:Object.values(db.videos).sort((a,b)=>b.created-a.created)
        });
        if (req.method==='POST' && url.pathname==='/api/profile') {
            const fields = await readProfile(req);
            const existing = req.headers.authorization ? authenticated(req) : null;
            nameAvailable(fields.user,existing?.id);
            const id = existing?.id || crypto.randomUUID();
            const token = existing ? null : crypto.randomBytes(32).toString('hex');
            const person = {...fields,id,tokenHash:existing?.tokenHash || digest(token)};
            db.people[id] = person;
            try { save(); } catch(err) { if (existing) db.people[id]=existing; else delete db.people[id]; throw err; }
            return reply(res,existing?200:201,{person:publicUser(person),...(token?{token}:{})});
        }
        if (req.method==='POST' && url.pathname==='/api/video') return await upload(req,res,url);
        const file = /^\/media\/([a-f0-9]{32}\.(?:mp4|webm))$/.exec(url.pathname);
        if (file && ['GET','HEAD'].includes(req.method)) return play(req,res,file[1]);
        throw error(404,'Endpoint not found');
    } catch(err) {
        if (!res.headersSent && !res.destroyed) reply(res,err.status||500,{error:err.status?err.message:'Server could not complete the request.'});
        else if (!res.destroyed) res.destroy();
    }
});
server.listen(Number(process.env.PORT || 8080),process.env.HOST || '0.0.0.0',()=>console.log('SP Shorts server listening on port ' + server.address().port));
