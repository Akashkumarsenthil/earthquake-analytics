export const FEED='https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson';
const number=x=>typeof x==='number'&&Number.isFinite(x);
export function normalize(data){
 if(data?.type!=='FeatureCollection'||!Array.isArray(data.features)||!number(data.metadata?.generated))throw new Error('Invalid feed');
 const ids=new Map();let rejected=0,duplicates=0;
 for(const f of data.features){const p=f?.properties,c=f?.geometry?.coordinates;if(!f?.id||!p||!Array.isArray(c)||!number(p.time)||!number(c[0])||!number(c[1])||Math.abs(c[0])>180||Math.abs(c[1])>90){rejected++;continue;}
 const e={id:String(f.id),place:p.place||'Unnamed location',time:p.time,updated:number(p.updated)?p.updated:p.time,mag:number(p.mag)?p.mag:null,magType:p.magType||'unspecified',depth:number(c[2])?c[2]:null,lon:c[0],lat:c[1],status:p.status||'Unknown',url:typeof p.url==='string'&&p.url.startsWith('https://earthquake.usgs.gov/')?p.url:null};
 if(ids.has(e.id))duplicates++;
 if(!ids.has(e.id)||ids.get(e.id).updated<e.updated)ids.set(e.id,e);
 }
 return {events:[...ids.values()].sort((a,b)=>b.time-a.time),generated:data.metadata.generated,rejected,duplicates,input:data.features.length};
}
export function filterEvents(events,{hours=24,minimum=-2,query='',anchor=Date.now()}={}){return events.filter(e=>e.time>=anchor-hours*3600000&&e.time<=anchor&&(minimum===-2||e.mag!==null&&e.mag>=minimum)&&e.place.toLowerCase().includes(query.trim().toLowerCase()));}
export function stats(events){const mags=events.map(e=>e.mag).filter(number),depths=events.map(e=>e.depth).filter(number).sort((a,b)=>a-b),n=depths.length;return {count:events.length,largest:mags.length?Math.max(...mags):null,depth:n?(depths[Math.floor(n/2)]+depths[Math.floor((n-1)/2)])/2:null,strong:mags.filter(x=>x>=5).length};}
export function csv(events){const quote=x=>'"'+String(x??'').replaceAll('"','""')+'"';return ['event_id,time_utc,place,magnitude,depth_km,latitude,longitude',...events.map(e=>[e.id,new Date(e.time).toISOString(),/^[=+@\-\t\r]/.test(e.place)?"'"+e.place:e.place,e.mag,e.depth,e.lat,e.lon].map(quote).join(','))].join('\r\n');}
