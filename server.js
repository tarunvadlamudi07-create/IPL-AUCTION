const http=require('http'),fs=require('fs');
const {WebSocketServer}=require('ws');
const POOL=fs.readFileSync(__dirname+'/players.txt','utf8').replace(/\n/g,'').split(';').filter(Boolean).map(s=>{const a=s.split('|');const p={n:a[0],r:a[1],os:+a[2],b:+a[3]};p.s=a[4]==='M'?'M':(p.b<=50&&!p.os?'U':p.r);return p});
const SO=['M','B','W','A','O','U'];
const html=fs.readFileSync(__dirname+'/index.html');
const srv=http.createServer((q,r)=>{r.writeHead(200,{'Content-Type':'text/html'});r.end(html)});
const wss=new WebSocketServer({server:srv});
const rooms={};
const fmt=L=>L>=100?(+(L/100).toFixed(2))+' cr':L+' L';
const inc=b=>b<100?5:b<200?10:b<500?20:25;
const shuf=a=>{a=a.slice();for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]]}return a};
const maxBid=(R,t)=>t.purse-Math.max(0,R.cfg.mn-t.sq.length-1)*30;
const nextAmt=R=>R.cur.bidder==null?R.cur.p.b:R.cur.bid+inc(R.cur.bid);

function mk(cfg,names){
  const code=Math.random().toString(36).slice(2,6).toUpperCase();
  return rooms[code]={code,cfg,T:names.map(n=>({n,purse:cfg.pu,sq:[],os:0,owner:null})),Q:[],ix:0,cur:null,unsold:[],r2:0,log:[],status:'lobby',paused:false,timer:0,msg:'',clients:new Set(),hostId:null,iv:null};
}
function push(R){
  for(const c of R.clients){
    if(c.readyState!==1)continue;
    c.send(JSON.stringify({code:R.code,status:R.status,cfg:R.cfg,
      T:R.T.map(t=>({n:t.n,purse:t.purse,os:t.os,sq:t.sq,taken:!!t.owner})),
      cur:R.cur,ix:R.ix,ql:R.Q.length,timer:R.timer,paused:R.paused,msg:R.msg,r2:R.r2,log:R.log.slice(0,10),
      me:{team:R.T.findIndex(t=>t.owner===c.pid),host:R.hostId===c.pid}}));
  }
}
function why(R,i,amt){
  const t=R.T[i],c=R.cfg;
  if(R.cur.bidder===i)return t.n+' already holds the highest bid.';
  if(t.sq.length>=c.mx)return t.n+' has a full squad.';
  if(R.cur.p.os&&t.os>=c.ov)return t.n+' already has '+c.ov+' overseas players.';
  if(amt>maxBid(R,t))return t.n+' can bid at most '+fmt(Math.max(0,maxBid(R,t)))+'.';
  return '';
}
function startRoom(R){
  R.Q=[];SO.forEach(s=>R.Q=R.Q.concat(shuf(POOL.filter(p=>p.s===s))));
  R.ix=0;R.cur={p:R.Q[0],bid:0,bidder:null};R.timer=R.cfg.tm+10;R.status='live';
  R.iv=setInterval(()=>{if(R.status!=='live'||R.paused)return;R.timer--;if(R.timer<=0)sell(R);else push(R)},1000);
  push(R);
}
function sell(R){
  const p=R.cur.p;
  if(R.cur.bidder!=null){const t=R.T[R.cur.bidder];t.sq.push({...p,price:R.cur.bid});t.purse-=R.cur.bid;t.os+=p.os;R.log.unshift('SOLD: '+p.n+' to '+t.n+' for '+fmt(R.cur.bid))}
  else{R.unsold.push(p);R.log.unshift('UNSOLD: '+p.n)}
  advance(R);
}
function skipSet(R){
  const s=R.cur.p.s;R.unsold.push(R.cur.p);
  while(R.ix+1<R.Q.length&&R.Q[R.ix+1].s===s){R.ix++;R.unsold.push(R.Q[R.ix])}
  R.log.unshift('Set skipped');advance(R);
}
function advance(R){
  R.ix++;
  if(R.ix>=R.Q.length){
    if(R.unsold.length&&R.r2<2){R.r2++;R.Q=shuf(R.unsold);R.unsold=[];R.ix=0;R.log.unshift('Accelerated round '+R.r2)}
    else{R.status='done';R.cur=null;clearInterval(R.iv);push(R);return}
  }
  R.cur={p:R.Q[R.ix],bid:0,bidder:null};R.timer=R.cfg.tm+10;R.msg='';push(R);
}
wss.on('connection',ws=>{
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.id)ws.pid=m.id;
    let R=ws.room;
    if(m.t==='create'){
      const c=m.cfg;
      if(!(c.n>=2&&c.n<=10&&c.n*c.mn<=POOL.length))return;
      R=ws.room=mk({n:c.n,pu:c.pu*100,mn:c.mn,mx:c.mx,ov:c.ov,tm:Math.max(8,c.tm)},m.names.slice(0,c.n));
      R.hostId=ws.pid;R.clients.add(ws);push(R);return;
    }
    if(m.t==='join'){R=rooms[(m.room||'').toUpperCase()];if(!R)return;ws.room=R;R.clients.add(ws);push(R);return}
    if(!R)return;
    const host=R.hostId===ws.pid;
    if(m.t==='claim'&&R.status==='lobby'){
      const t=R.T[m.team];if(!t||(t.owner&&t.owner!==ws.pid))return;
      R.T.forEach(x=>{if(x.owner===ws.pid)x.owner=null});t.owner=ws.pid;push(R);
    }
    else if(m.t==='start'&&host&&R.status==='lobby'&&R.T.some(t=>t.owner))startRoom(R);
    else if(m.t==='bid'&&R.status==='live'&&!R.paused){
      const i=R.T.findIndex(t=>t.owner===ws.pid);if(i<0)return;
      const a=nextAmt(R),w=why(R,i,a);
      if(w){ws.send(JSON.stringify({err:w}));return}
      R.cur.bid=a;R.cur.bidder=i;R.timer=R.cfg.tm;R.msg=R.T[i].n+' bids '+fmt(a);push(R);
    }
    else if(host&&R.status==='live'){
      if(m.t==='pause'){R.paused=!R.paused;push(R)}
      else if(m.t==='hammer')sell(R);
      else if(m.t==='skip')skipSet(R);
    }
  });
  ws.on('close',()=>{if(ws.room)ws.room.clients.delete(ws)});
});
srv.listen(process.env.PORT||3000,()=>console.log('Auction server running'));
