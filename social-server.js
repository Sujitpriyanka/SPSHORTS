'use strict';
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const authOrigin=process.env.AUTH_ORIGIN || 'https://sp-shorts.onrender.com';
const dataDir=path.resolve(process.env.SOCIAL_DATA || 'social-data');fs.mkdirSync(dataDir,{recursive:true});
const dbPath=path.join(dataDir,'events.json');
let db=fs.existsSync(dbPath)?JSON.parse(fs.readFileSync(dbPath,'utf8')):{seen:{},active:{},events:{},removed:{}};
const epoch=crypto.randomUUID(),authCache=new Map(),limits=new Map();
const error=(status,message)=>Object.assign(Error(message),{status});
const send=(res,status,data)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(data));};
const hash=text=>crypto.createHash('sha256').update(text).digest('hex');
function save(){fs.writeFileSync(dbPath+'.tmp',JSON.stringify(db),{mode:0o600});fs.renameSync(dbPath+'.tmp',dbPath);}
async function body(req){let bytes=0,parts=[];for await(const part of req){bytes+=part.length;if(bytes>65536)throw error(413,'Request too large');parts.push(part);}try{return JSON.parse(Buffer.concat(parts).toString('utf8'));}catch{throw error(400,'Invalid request');}}
async function fetchJSON(url,options={}){let response;try{response=await fetch(url,{...options,signal:AbortSignal.timeout(95000)});}catch{throw error(503,'Account server is waking up. Try again.');}let result;try{result=await response.json();}catch{throw error(503,'Account service unavailable');}if(!response.ok)throw error(response.status===401?401:503,response.status===401?'Connect your account again.':'Account service unavailable');return result;}
async function catalog(){return fetchJSON(authOrigin+'/api/catalog');}
async function authenticated(req,url){
 const id=url.searchParams.get('account') || '',token=(req.headers.authorization || '').replace(/^Bearer /,'');
 if(!/^[a-f0-9-]{36}$/.test(id)||!/^[a-f0-9]{64}$/.test(token))throw error(401,'Connect your account first.');
 const key=hash(id+':'+token),cached=authCache.get(key);if(cached&&cached.until>Date.now())return cached.person;
 const data=await catalog(),person=data.users.find(p=>p.id===id);if(!person)throw error(401,'Account not found on profile server.');
 // The legacy profile service has no read-only token-check endpoint. A no-op update
 // of the freshly fetched profile proves the token's account identity.
 const checked=await fetchJSON(authOrigin+'/api/profile',{method:'POST',headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify({user:person.user,name:person.name,bio:person.bio,avatar:person.avatar})});
 if(checked.person.id!==id)throw error(403,'This account does not match your token.');
 authCache.set(key,{person:checked.person,until:Date.now()+60000});return checked.person;
}
function rate(id){const now=Date.now(),old=limits.get(id);if(!old||old.until<now){limits.set(id,{until:now+60000,count:1});return;}if(++old.count>180)throw error(429,'Please wait before trying again.');}
function applyEvent(actor,input,users,videos){
 const {id,type,targetId}=input;
 if(!/^[a-zA-Z0-9_-]{8,100}$/.test(id||'')||!['follow','like','comment'].includes(type))throw error(400,'Invalid activity');
 const unique=actor.id+':'+id;if(db.seen[unique])return;
 const target=users.find(p=>p.id===targetId);if(!target)throw error(404,'Recipient account not found.');
 if(target.id===actor.id){db.seen[unique]=Date.now();return;}
 let video;
 if(type!=='follow'){
  video=videos.find(v=>v.id===input.videoId && v.ownerId===target.id);if(!video)throw error(404,'This video is not published on the server yet.');
 }
 const pair=actor.id+':'+type+':'+(video?.id || target.id);
 if(type!=='comment' && input.enabled===false){
  const previous=db.active[pair];if(previous){delete db.events[previous];db.removed[previous]={targetId,at:Date.now()};delete db.active[pair];}
  db.seen[unique]=Date.now();return;
 }
 if(type!=='comment' && db.active[pair]){db.seen[unique]=Date.now();return;}
 const text=type==='comment'?String(input.text || '').trim().slice(0,500):'';
 if(type==='comment'&&!text)throw error(400,'Write a comment first.');
 const notificationId=hash(unique).slice(0,32);
 db.events[notificationId]={id:notificationId,type,actorId:actor.id,actor:{user:actor.user,name:actor.name,avatar:actor.avatar || ''},targetId,videoId:video?.id || '',videoTitle:video?.title || '',text,created:Math.min(Date.now(),Math.max(Date.now()-30*86400000,Number(input.created)||Date.now())),read:false};
 if(type!=='comment')db.active[pair]=notificationId;
 db.seen[unique]=Date.now();
}
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://localhost');
 if(req.method==='GET'&&url.pathname==='/healthz')return send(res,200,{ok:true,service:'sp-shorts-notifications',epoch});
 if(!['/api/social/notifications','/api/social/events','/api/social/read'].includes(url.pathname))throw error(404,'Endpoint not found');
 const actor=await authenticated(req,url);rate(actor.id);
 if(req.method==='GET'&&url.pathname==='/api/social/notifications'){
  const events=Object.values(db.events).filter(n=>n.targetId===actor.id).sort((a,b)=>b.created-a.created).slice(0,200);
  return send(res,200,{epoch,events,removed:Object.entries(db.removed).filter(([,n])=>n.targetId===actor.id).map(([id])=>id)});
 }
 if(req.method!=='POST')throw error(405,'Method not allowed');
 const data=await body(req);
 const metadata=url.pathname==='/api/social/events'?await catalog():null;
 const previous=JSON.stringify(db);
 try{
  if(url.pathname==='/api/social/events'){
   const inputs=Array.isArray(data.events)?data.events:[data];if(inputs.length>50)throw error(400,'Too many events');
   const dataCatalog=metadata;
   for(const input of inputs)applyEvent(actor,input,dataCatalog.users,dataCatalog.videos);
   save();return send(res,200,{ok:true,epoch,accepted:inputs.map(input=>input.id)});
  }
  if(url.pathname==='/api/social/read'){
   if(!Array.isArray(data.ids)||data.ids.length>200)throw error(400,'Invalid notification list');
   for(const id of data.ids)if(db.events[id]?.targetId===actor.id)db.events[id].read=true;
   save();return send(res,200,{ok:true,epoch});
  }
  throw error(405,'Method not allowed');
 }catch(err){db=JSON.parse(previous);throw err;}
}catch(err){if(!res.headersSent)send(res,err.status||500,{error:err.status?err.message:'Notification service could not complete this request.'});}});
server.listen(Number(process.env.PORT || 8081),'0.0.0.0',()=>console.log('SP Shorts notifications ready'));
