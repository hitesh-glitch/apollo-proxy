

const DEFAULT_PROXY = 'http://localhost:8765';
let PROXY = localStorage.getItem('dmand_proxy_url') || DEFAULT_PROXY;
const LS_KEY = 'dmand_v2';

const IST_OPTS_DATETIME = { timeZone:'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric', hour:'2-digit', minute:'2-digit', hour12:true };
const IST_OPTS_DATE     = { timeZone:'Asia/Kolkata', day:'2-digit', month:'short', year:'numeric' };
const IST_OPTS_TIME     = { timeZone:'Asia/Kolkata', hour:'2-digit', minute:'2-digit', hour12:true };

function istDateTime(iso) {
  if(!iso) return '—';
  try { return new Date(iso).toLocaleString('en-IN', IST_OPTS_DATETIME)+' IST'; }
  catch(e){ return iso; }
}
function istDate(iso) {
  if(!iso) return '—';
  try {
    const d = iso.includes('T') ? new Date(iso) : new Date(iso+'T00:00:00+05:30');
    return d.toLocaleDateString('en-IN', IST_OPTS_DATE);
  } catch(e){ return iso; }
}
function istTime(iso) {
  if(!iso) return '—';
  try { return new Date(iso).toLocaleTimeString('en-IN', IST_OPTS_TIME)+' IST'; }
  catch(e){ return iso; }
}

let DB = {
  signals:[], events:[], keys:[], personas:[], contacts:[], companies:[],
  autoboundKeys: [],
  heyreachKeys: [],
  settings:{ pollInterval:30, lookback:'3d', apolloKey:'', fullenrichKey:'', crustdataKey:'', oaiKey:'', proxyUrl:'' },
  outreach:{ smartleadKey:'', campaignSignalId:null, campaignIcebreakerIdId:null,
    companyBrief:'', valueProp:'', painPoints:'', cta:'Book a 15-min call' },
  campaigns: [],      // Smartlead campaigns
  hrCampaigns: [],    // HeyReach campaigns
  meta:{ activeKeyIdx:0, lastPoll:null }
};
let pollTimer = null;
let pollPaused = false;
let pollRunning = false; // guard against concurrent polls
let currentPage = 'home'; // track active page for render skip optimization
var _expandedEventGroups = {}; // domain -> bool (undefined=expanded by default)
var _expandedEvents = {};      // event_id -> bool
let proxyOk = false;
let showCompletions = false;
let campFilter = 'all'; // 'all' | 'smartlead' | 'heyreach'

let modalEventId = null;

let liModalContactId = null;
let liModalContactIds = [];
let liModalCampaignId = null;
let liModalCampaignName = '';
let liGeneratedMessages = null;

const TEMPLATES = [
  { name:'Cardiac monitoring expansion', category:'Expansion signal', frequency:'1d',
    query:'Alert me when hospitals, health systems, or cardiology practices announce new remote cardiac monitoring programs, wearable ECG adoption, or RPM partnerships — especially academic medical centers or large IDNs.' },
  { name:'Robotic surgery adoption', category:'New adopter signal', frequency:'1d',
    query:'Notify me when hospitals or surgical centers announce plans to adopt robotic-assisted surgery, purchase surgical robotics systems, or partner with robotic surgery vendors.' },
  { name:'Spine surgery expansion', category:'Expansion signal', frequency:'3d',
    query:'Alert me when spine surgery centers, orthopedic practices, or hospitals announce expansions of spine programs, new spine surgeon hires, or high-volume spine procedure announcements.' },
  { name:'Hospital VAC decisions', category:'Regulatory signal', frequency:'1w',
    query:'Track when hospitals or health systems publish medical device purchasing policy changes, value analysis committee decisions, or formulary updates for surgical or implantable devices.' },
  { name:'Competitive displacement wins', category:'Competitive signal', frequency:'1d',
    query:'Alert me when medical device companies announce competitive displacement wins, contract changes at health systems, or hospitals switching vendors for surgical or monitoring equipment.' },
  { name:'New hospital OR expansions', category:'Expansion signal', frequency:'1w',
    query:'Notify me when hospitals or health systems announce new facility construction, OR expansions, new service line launches, or capital equipment procurement for surgical services.' },
  { name:'Payer coverage changes', category:'Regulatory signal', frequency:'1w',
    query:'Alert me when major commercial payers or CMS announce new coverage policies, reimbursement changes, or prior auth updates affecting cardiac monitoring, surgical robotics, or implantable devices.' },
  { name:'FDA clearance adoption', category:'New adopter signal', frequency:'3d',
    query:'Track when recently FDA-cleared medical devices receive early hospital adopter announcements, pilot program launches, or clinical evaluation agreements at health systems.' }
];

function runContentDedup() {
  var seen = new Set();
  var before = DB.events.length;
  DB.events = DB.events.filter(function(e) {
    if(e.type !== 'event') return true;
    var fp = ((e.output||'').slice(0,80).replace(/\s+/g,' ').trim() + '|' + (e.event_date||'')).toLowerCase();
    if(seen.has(fp)) return false;
    seen.add(fp);
    return true;
  });
  var removed = before - DB.events.length;
  if(removed > 0) {
    saveAndSync();
    log('Removed ' + removed + ' duplicate event(s) — clean copy synced to cloud', 'amber');
  }
}

function init() {
  // Dismiss loader immediately — show UI even if errors occur later
  requestAnimationFrame(function(){
    var loader=document.getElementById('app-loader');
    if(loader){loader.style.opacity='0';setTimeout(function(){loader.style.display='none';},300);}
  });
  load();
  cloudLoad().then(function(loaded){
    if(loaded){
      runContentDedup();
      renderAll(); updateMetrics(); updateKeySidebarDot(); startCloudAutoSave();
      populateIcpSignalDropdown();
      if(DB.settings&&DB.settings.proxyUrl){
        if(!localStorage.getItem('dmand_proxy_url')||localStorage.getItem('dmand_proxy_url')===DEFAULT_PROXY){
          localStorage.setItem('dmand_proxy_url',DB.settings.proxyUrl);
          PROXY=DB.settings.proxyUrl;
          var proxyEl=document.getElementById('s-proxy-url');
          if(proxyEl) proxyEl.value=DB.settings.proxyUrl;
        }
      }
      DB.events.forEach(function(e){
        if(e.type==='event' && e.qualified===undefined){
          e.qualified='unknown';
        }
      });
      setTimeout(function(){
        var superPending=DB.events.filter(function(e){
          return e.type==='event'
            && e.enrichment&&e.enrichment.status==='done'
            && e.qualified==='yes'  // only retry if ICP qualified
            && e.superEnrichment&&e.superEnrichment.status==='error'
            && !(e.superEnrichment.message&&e.superEnrichment.message.includes('402')) // skip credit errors
            && !(e.superEnrichment.exhausted); // skip permanently exhausted
        });
        if(superPending.length&&!window._superRetryDone){
          window._superRetryDone=true;
          log('Auto-retrying '+superPending.length+' failed Super Enrich(es) after cloud load...','amber');
          superPending.forEach(function(e,i){
            setTimeout(function(){ superEnrichEvent(e.event_id); }, i*2000);
          });
        }
      }, 3000);
    }
    updateCloudDot();
  });
  ['home','signals','events','companies','contacts','campaigns','keys','icp','settings'].forEach(function(p){
    var el=document.getElementById('page-'+p);
    if(el) el.style.display='none';
  });
  document.getElementById('page-home').style.display='';
  initUsage();
  if(!DB.personas) DB.personas=[];
  if(!DB.contacts) DB.contacts=[];
  if(!DB.companies) DB.companies=[];
  if(!DB.autoboundKeys) DB.autoboundKeys=[];
  if(!DB.heyreachKeys) DB.heyreachKeys=[];
  if(!DB.hrCampaigns) DB.hrCampaigns=[];
  if(DB.settings.clayKey&&!DB.settings.fullenrichKey){ DB.settings.fullenrichKey=DB.settings.clayKey; delete DB.settings.clayKey; save(); }

  var senMap={
    'Owner / Partner':'owner','CXO':'c_suite','Vice President':'vp',
    'Experienced Manager':'manager','Strategic':'senior','Entry Level Manager':'entry',
    'Founder':'founder','Owner':'owner','C-Suite':'c_suite','VP':'vp',
    'Manager':'manager','Senior':'senior','Entry':'entry','Entry Level':'entry',
    'Director':'director','Head':'head',
    'founder':'founder','owner':'owner','c_suite':'c_suite','partner':'partner',
    'vp':'vp','head':'head','director':'director','manager':'manager',
    'senior':'senior','entry':'entry','intern':'intern'
  };
  (DB.personas||[]).forEach(function(p){
    if(p.seniority&&p.seniority!=='any'){
      var vals=p.seniority.split(',').map(function(s){return s.trim();});
      var fixed=vals.map(function(v){return senMap[v]||v;});
      var newSen=fixed.join(',');
      if(newSen!==p.seniority){
        p.seniority=newSen;
        log('Migrated persona "'+p.name+'" seniority: '+p.seniority+' → '+newSen,'gray');
      }
    }
  });

  if(!DB.outreach) DB.outreach={smartleadKey:'',campaignSignalId:null,campaignIcebreakerIdId:null,companyBrief:'',valueProp:'',painPoints:'',cta:'Book a 15-min call'};
  if(!DB.campaigns) DB.campaigns=[];
  DB.events.forEach(e=>{
    if(e.enrichment&&e.enrichment.status!=='done') e.enrichment=null;
  });
  let migrated = 0;
  DB.contacts.forEach(c=>{
    const td = ((c.title||'') + ' ' + (c.department||'')).toLowerCase();
    if(!c.seniority || c.seniority === 'Individual Contributor'){
      if(/\bchief\b|\bceo\b|\bcoo\b|\bcfo\b|\bcto\b|\bcmo\b|\bcro\b/.test(td))     c.seniority = 'C-Suite';
      else if(/\bsvp\b|\bevp\b|\bvp\b|\bvice president\b/.test(td))                 c.seniority = 'VP';
      else if(/\bdirector\b/.test(td))                                               c.seniority = 'Director';
      else if(/\bhead of\b|\bmanager\b/.test(td))                                   c.seniority = 'Manager';
      else if(/\bsenior\b|\bsr\.\b|\blead\b|\bprincipal\b/.test(td))               c.seniority = 'Senior IC';
      migrated++;
    }
    if(c.title && (c.title.length > 100 || /^(a |an |the |with |i |as )/i.test(c.title))){
      const raw = c.title;
      const asAt = raw.match(/\bas\s+([^,.]{5,80}?)\s+at\s+/i);
      if(asAt) c.title = asAt[1].trim();
      else {
        const isA = raw.match(/(?:is\s+(?:a|an|the)\s+)([^,.]{5,80}?)(?:\s+at|\s+with|\.|,)/i);
        if(isA) c.title = isA[1].trim();
      }
    }
  });
  if(migrated > 0 && window._localDataExists) save();

  let companyMigrated = 0;
  DB.events.forEach(ev => {
    if(!ev.superEnrichment || ev.superEnrichment.status !== 'done') return;
    const d = ev.superEnrichment.data;
    if(!d) return;
    const domain = d.domain || ev.enrichment?.data?.domain;
    if(!domain) return;
    const existing = DB.companies.find(c => c.domain === domain);
    const entry = {
      id: existing?.id || ('co_'+Date.now()+'_'+Math.random().toString(36).slice(2,5)),
      domain,
      event_ids:    [...new Set([...(existing?.event_ids||[]), ev.event_id])],
      signal_names: [...new Set([...(existing?.signal_names||[]), ev.signal_name||''])],
      enriched_at:  ev.superEnrichment.enriched_at || new Date().toISOString(),
      ...d
    };
    if(existing){ Object.assign(existing, entry); }
    else { DB.companies.push(entry); companyMigrated++; }
  });
  if(companyMigrated > 0 && window._localDataExists){
    save();
    log('Rebuilt '+companyMigrated+' company record(s) from event enrichment data', 'amber');
  }

  var deptMigrated = 0;
  DB.contacts.forEach(function(c){
    if(c.department) return;
    var tl = ((c.title||'') + ' ' + (c.headline||'')).toLowerCase();
    if(/\bsales\b|\baccount exec|\baccount manager|\bbusiness dev|\bbd\b|\bsdr\b|\bae\b/.test(tl))         c.department = 'Sales';
    else if(/\bmarket|\bgrowth|\bdemand gen|\bcontent|\bseo\b|\bpr\b|\bcommunity/.test(tl))               c.department = 'Marketing';
    else if(/\bengineering|\bsoftware|\bdeveloper|\bdevops|\binfra|\bsre\b|\bbackend|\bfrontend/.test(tl)) c.department = 'Engineering';
    else if(/\bproduct\b|\bpm\b|\bprogram manager/.test(tl))                                             c.department = 'Product';
    else if(/\bfinance|\bfinancial|\baccounting|\bcfo\b|\bcontroller/.test(tl))                          c.department = 'Finance';
    else if(/\boperation|\bops\b|\bcoo\b|\bsupply chain/.test(tl))                                      c.department = 'Operations';
    else if(/\bcustomer success|\bclient|\bsupport/.test(tl))                                           c.department = 'Customer Success';
    else if(/\bhr\b|\bhuman res|\bpeople ops|\btalent|\brecruit/.test(tl))                               c.department = 'Human Resources';
    else if(/\bdata sci|\bdata eng|\bmachine learn|\bai\b|\bml\b|\banalyst/.test(tl))                   c.department = 'Data Science';
    else if(/\bdesign|\bux\b|\bui\b|\bvisual|\bbrand/.test(tl))                                         c.department = 'Design';
    else if(/\blegal|\bcounsel|\bcompliance/.test(tl))                                                   c.department = 'Legal';
    else if(/\bceo|\bcto|\bcmo|\bcoo|\bcfo|\bchief|\bpresident|\bfounder/.test(tl))                     c.department = 'Executive';
    if(c.department) deptMigrated++;
  });
  if(deptMigrated > 0 && window._localDataExists){ save(); console.log('[init] inferred dept for', deptMigrated, 'contacts'); }

  var sigMigrated = 0;
  DB.contacts.forEach(function(c){
    if(c.source !== 'crustdata') return;
    if(c.signal_name && c.event_brief) return;
    var companyRec = DB.companies.find(function(co){ return co.domain === c.domain; });
    if(!companyRec) return;
    if(!c.signal_name) c.signal_name = (companyRec.signal_names||[]).join(', ');
    if(!c.event_brief && companyRec.event_ids && companyRec.event_ids.length){
      var ev = DB.events.find(function(e){ return e.event_id === companyRec.event_ids[0]; });
      if(ev) c.event_brief = (ev.output||'').replace(/\s+/g,' ').trim().slice(0,300);
    }
    sigMigrated++;
  });
  if(sigMigrated > 0 && window._localDataExists){ save(); console.log('[init] backfilled signal/brief for', sigMigrated, 'contacts'); }

  initTheme(); initSidebarState(); migratePersonaFunctions(); renderAll(); loadSettingsUI(); startPollTimer(); renderTemplates(); loadOAIKeyUI(); loadFullenrichKeyUI(); loadCrustdataKeyUI(); loadAutoboundKeysUI(); loadHeyreachKeysUI(); loadSmartleadKeyUI(); loadOutreachSettingsUI(); renderCampaignList();
  showPage('home', document.querySelector('.nav-item'));
  var oldJunkCount=DB.events.filter(function(e){return e.type==='completion'||e.type==='error';}).length;
  if(oldJunkCount){ DB.events=DB.events.filter(function(e){return e.type!=='completion'&&e.type!=='error';}); if(DB.signals.length||DB.events.length) save(); renderEvents(); log('Purged '+oldJunkCount+' stale completion/error event(s) from storage (dedup fix)','amber'); }

  runContentDedup();
  setTimeout(function(){
    var pending=DB.events.filter(function(e){return e.type==='event'&&(!e.enrichment||e.enrichment.status==='error');});
    if(pending.length){
      log('Auto-enriching '+pending.length+' pending event(s)...','gray');
      pending.forEach(function(e,i){ setTimeout(function(){ enrichEvent(e.event_id); },i*1200); });
    }
    var superPending=DB.events.filter(function(e){
      return e.type==='event'
        && e.enrichment&&e.enrichment.status==='done'
        && e.qualified==='yes'  // only ICP-qualified events
        && e.superEnrichment&&e.superEnrichment.status==='error'  // failed (not credit_error)
        && !(e.superEnrichment.message&&e.superEnrichment.message.includes('402'))
        && !(e.superEnrichment.exhausted); // skip permanently exhausted
    });
    if(superPending.length&&!window._superRetryDone){
      window._superRetryDone=true;
      log('Auto-retrying '+superPending.length+' failed Super Enrich(es)...','amber');
      var baseDelay=(pending.length*1200)+3000; // wait for standard enrichments first
      superPending.forEach(function(e,i){
        setTimeout(function(){ superEnrichEvent(e.event_id); }, baseDelay+(i*2000));
      });
    }
  }, 2000);
  checkProxy(); setInterval(checkProxy, 15000);
  updateKeySidebarDot();
  updateHrSidebarDot();
  loadApolloKeyUI();
  updateApolloSidebarDot();
  populateIcpSignalDropdown();
  checkMonitorHealth(false);
  setInterval(function(){ checkMonitorHealth(false); }, 60000);
  // Dismiss loading screen once app is ready
  requestAnimationFrame(function(){
    var loader=document.getElementById('app-loader');
    if(loader){loader.style.opacity='0';setTimeout(function(){loader.style.display='none';},350);}
  });
}

let cloudSyncEnabled = false;
let cloudSyncPending = false;
let cloudSyncTimer  = null;
let cloudAutoSaveTimer = null;
const CLOUD_AUTOSAVE_INTERVAL = 60 * 60 * 1000; // 60 minutes

async function cloudLoad() {
  if(!PROXY || PROXY === DEFAULT_PROXY) return false; // only cloud workers have KV
  if(window._cloudLoadInProgress) return false;
  window._cloudLoadInProgress=true;
  setTimeout(function(){ window._cloudLoadInProgress=false; },5000);
  try {
    const r = await fetch(PROXY+'/kv/dmand_v2', { signal: AbortSignal.timeout(8000) });
    if(!r.ok) return false;
    const data = await r.json();
    if(data && typeof data === 'object' && (data.signals||data.events)) {
      DB = {...DB, ...data};
      if(!DB.heyreachKeys) DB.heyreachKeys=[];
      if(!DB.hrCampaigns) DB.hrCampaigns=[];
      window._localDataExists=true; // cloud data now in DB
      window._justLoadedFromCloud=true;
      save(); // Write cloud data to localStorage immediately
      setTimeout(function(){window._justLoadedFromCloud=false;},10000);
      cloudSyncEnabled = true;

      var beforeMigrate = JSON.stringify(DB.personas||[]);
      migratePersonaFunctions();
      if(JSON.stringify(DB.personas||[]) !== beforeMigrate){
        log('Persona function migration applied — saving','amber');
        setTimeout(function(){ cloudSave(); }, 500);
      }
      log('☁ Loaded from Cloudflare KV','green');
      setTimeout(cloudLoadLogs, 1500);
      return true;
    }
  } catch(e) { log('KV load skipped: '+e.message,'gray'); }
  return false;
}

async function cloudSave() {
  if(!cloudSyncEnabled) return;
  try {
    const r = await fetch(PROXY+'/kv/dmand_v2', {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(DB),
      signal: AbortSignal.timeout(10000)
    });
    if(r.ok) {
      const ts = istTime(new Date().toISOString());
      document.getElementById('save-label').textContent='Saved · ☁ '+ts;
      cloudSyncPending = false;
      log('☁ Cloud saved at '+ts+' ('+Math.round(JSON.stringify(DB).length/1024)+'KB)','gray');
      cloudSaveLogs().catch(function(){});
    } else {
      log('☁ Cloud save failed: HTTP '+r.status,'red');
    }
  } catch(e) { log('☁ Cloud save error: '+e.message,'red'); }
}

async function cloudSaveLogs() {
  if(!cloudSyncEnabled||!PROXY||PROXY===DEFAULT_PROXY) return;
  if(!DB.log||!DB.log.length) return;
  try {
    await fetch(PROXY+'/kv/dmand_logs', {
      method:'PUT',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({logs:DB.log, savedAt:new Date().toISOString()}),
      signal:AbortSignal.timeout(8000)
    });
  } catch(e){  }
}

async function cloudLoadLogs() {
  if(!cloudSyncEnabled||!PROXY||PROXY===DEFAULT_PROXY) return;
  try {
    const r = await fetch(PROXY+'/kv/dmand_logs', { signal:AbortSignal.timeout(6000) });
    if(!r.ok) return;
    const data = await r.json();
    if(!data||!Array.isArray(data.logs)||!data.logs.length) return;
    var existing = DB.log||[];
    var combined = [...existing];
    data.logs.forEach(function(entry){
      var dup = existing.some(function(e){ return e.t===entry.t && e.msg===entry.msg; });
      if(!dup) combined.push(entry);
    });
    combined.sort(function(a,b){ return 0; }); // preserve insertion order
    combined = combined.slice(0, MAX_LOG);
    DB.log = combined;
    save();
    renderLog();
    log('☁ Activity log restored ('+data.logs.length+' entries from '+
      (data.savedAt?istTime(data.savedAt):'cloud')+')','gray');
  } catch(e){  }
}

function startCloudAutoSave() {
  if(cloudAutoSaveTimer) clearInterval(cloudAutoSaveTimer);
  cloudAutoSaveTimer = setInterval(async function() {
    if(!cloudSyncEnabled) return;
    log('☁ Auto cloud save (60min)...', 'gray');
    await cloudSave();
    log('☁ Auto cloud save complete', 'green');
  }, CLOUD_AUTOSAVE_INTERVAL);
  log('☁ Cloud auto-save every 60 min active', 'gray');
}

async function forceCloudSave() {
  if(!cloudSyncEnabled) {
    showAlert('Cloud sync not active — migrate your data first (Settings → Cloud Storage).', 'warning', 4000);
    return;
  }
  const btn = document.getElementById('force-cloud-save-btn');
  if(btn) { btn.disabled = true; btn.textContent = '☁ Saving…'; }
  await cloudSave();
  if(btn) { btn.disabled = false; btn.textContent = '☁ Force save'; }
  showAlert('☁ Force saved to Cloudflare KV.', 'success', 3000);
}

function load() {
  try {
    const r=localStorage.getItem(LS_KEY);
    if(r){ DB={...DB,...JSON.parse(r)}; migratePersonaFunctions(); window._localDataExists=true; }
    else { window._localDataExists=false; } // fresh file — don't save until cloud loads
  } catch(e){ window._localDataExists=false; }
}

function saveAndSync() {
  save(); // localStorage + starts debounce
  if(cloudSyncEnabled) {
    clearTimeout(cloudSyncTimer);
    cloudSyncTimer = null;
    cloudSyncPending = false;
    cloudSave(); // fire immediately
  }
}

var _lsTimer=null;
function save() {
  try {
    localStorage.setItem(LS_KEY, JSON.stringify(DB));
    var el=document.getElementById('save-label');
    if(el) el.textContent='Saved · '+istTime(new Date().toISOString());
  } catch(e){ showAlert('localStorage full! Export a backup.','error',0); }
  if(cloudSyncEnabled && !window._justLoadedFromCloud) {
    cloudSyncPending = true;
    clearTimeout(cloudSyncTimer);
    cloudSyncTimer = setTimeout(cloudSave, 3000);
  }
}

async function migrateToCloud() {
  if(!PROXY || PROXY === DEFAULT_PROXY) {
    showAlert('Set your Cloudflare Worker URL in Settings → Proxy URL first.','error',0);
    return;
  }
  const btn = document.getElementById('migrate-btn');
  const status = document.getElementById('migrate-status');
  if(btn) btn.disabled = true;
  if(status) status.textContent = 'Uploading…';
  try {
    const r = await fetch(PROXY+'/kv/dmand_v2', {
      method: 'PUT',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(DB),
      signal: AbortSignal.timeout(15000)
    });
    if(r.ok) {
      cloudSyncEnabled = true;
      if(status) status.textContent = '✓ Migrated! '+DB.events.length+' events, '+DB.signals.length+' signals, '+DB.contacts.length+' contacts uploaded. Auto-saving every 60 min.';
      save();
      startCloudAutoSave();
      showAlert('☁ All data migrated to Cloudflare KV. Auto-saving every 60 min.','success',0);
      log('☁ Data migrated to Cloudflare KV','green');
    } else {
      const t = await r.text();
      if(status) status.textContent = '✗ Failed: HTTP '+r.status+' — '+t.slice(0,80);
      showAlert('Migration failed — check Worker KV binding (see instructions below).','error',0);
      if(btn) btn.disabled = false;
    }
  } catch(e) {
    if(status) status.textContent = '✗ Error: '+e.message;
    if(btn) btn.disabled = false;
  }
}

async function updateCloudDot() {
  const dot = document.getElementById('cloud-dot');
  const lbl = document.getElementById('cloud-label');
  const sidebarDot = document.getElementById('cloud-sidebar-dot');
  const saveLabel = document.getElementById('save-label');
  if(!cloudSyncEnabled) {
    if(dot) dot.style.background = 'var(--text3)';
    if(lbl) lbl.textContent = 'Cloud sync: not configured — set Cloudflare Worker URL above';
    if(sidebarDot) sidebarDot.style.background = 'var(--text3)';
  } else {
    if(dot) dot.style.background = 'var(--green)';
    if(lbl) lbl.textContent = 'Cloud sync: active ✓ — auto-saving every 60 min';
    if(sidebarDot) sidebarDot.style.background = 'var(--green)';
    if(saveLabel && !saveLabel.textContent.includes('·')) saveLabel.textContent = 'Saved · cloud ☁';
  }
}

async function checkProxy() {
  try {
    const res=await fetch(PROXY+'/v1alpha/monitors',{ method:'OPTIONS', signal:AbortSignal.timeout(3000) });
    setProxyStatus(true);
  } catch(e){
    if(e.name==='TypeError'||e.message.includes('Failed to fetch')||e.message.includes('NetworkError')){
      setProxyStatus(false);
    } else { setProxyStatus(true); }
  }
}

function saveApolloKey(){
  var val=document.getElementById('apollo-key-input').value.trim();
  if(!val){showAlert('Enter your Apollo Master API key.','error');return;}
  DB.settings.apolloKey=val;
  save();
  document.getElementById('apollo-key-input').value='';
  loadApolloKeyUI();
  updateApolloSidebarDot();
  showAlert('Apollo key saved.','success');
}

function clearApolloKey(){
  DB.settings.apolloKey='';
  save();
  loadApolloKeyUI();
  updateApolloSidebarDot();
  showAlert('Apollo key cleared.','info');
}

function loadApolloKeyUI(){
  var key=DB.settings.apolloKey||'';
  var statusEl=document.getElementById('apollo-key-status');
  var inputEl=document.getElementById('apollo-key-input');
  if(statusEl) statusEl.textContent=key?'Active ✓ ('+key.slice(0,6)+'...)':'Not set';
  if(statusEl) statusEl.style.color=key?'var(--green)':'var(--text3)';
  if(inputEl) inputEl.value='';
}

function updateApolloSidebarDot(){
  var dot=document.getElementById('apollo-sidebar-dot');
  var lbl=document.getElementById('apollo-sidebar-label');
  if(!dot||!lbl) return;
  var key=DB.settings.apolloKey||'';
  dot.style.background=key?'#6b21a8':'var(--text3)';
  lbl.textContent=key?'Apollo: active':'Apollo: not set';
}

function updateHrSidebarDot(){
  var dot=document.getElementById('hr-sidebar-dot');
  var lbl=document.getElementById('hr-sidebar-label');
  if(!dot||!lbl) return;
  var key=getActiveHeyreachKey();
  if(!key){
    dot.style.background='var(--text3)'; lbl.textContent='HeyReach: not set';
  } else {
    dot.style.background='#0a66c2'; lbl.textContent='HeyReach: active';
  }
}
function updateKeySidebarDot(){
  var dot=document.getElementById('key-status-dot');
  var lbl=document.getElementById('key-status-label');
  if(!dot||!lbl) return;
  if(!DB.keys.length){
    dot.className='dot dot-red'; lbl.textContent='Parallel key: not set';
  } else if(DB.keys[0].exhausted){
    dot.className='dot dot-red'; lbl.textContent='Parallel key: exhausted ⚠';
  } else {
    dot.className='dot dot-green'; lbl.textContent='Parallel key: active';
  }
}

function setProxyStatus(ok) {
  proxyOk=ok;
  document.getElementById('proxy-dot').className='dot '+(ok?'dot-green':'dot-red');
  document.getElementById('proxy-label').textContent='Proxy: '+(ok?'running ✓':'not running');
  document.getElementById('proxy-banner').classList.toggle('show',!ok);
}

var monitorHealthCache = {}; // monitor_id -> {status, last_run, error, checked_at}

async function fetchAllMonitorHealth(){
  if(!getActiveKey()) return;
  const btn=document.getElementById('health-refresh-btn');
  const el=document.getElementById('monitor-health-list');
  const dotEl=document.getElementById('health-panel-dot');
  const titleEl=document.getElementById('health-panel-title');
  const checkedEl=document.getElementById('health-panel-checked');
  if(btn){btn.textContent='Fetching...';btn.disabled=true;}
  if(el) el.innerHTML='<div style="display:flex;align-items:center;gap:6px;color:var(--text3)"><div class="spinner" style="width:10px;height:10px"></div>Fetching monitors from Parallel...</div>';

  const allRes=await pFetch('GET','/v1alpha/monitors');
  const active=DB.signals.filter(s=>s.status==='active'&&s.monitor_id);
  const healthResults=await Promise.all(active.map(async function(sig){
    try{
      const r=await fetch(PROXY+'/v1alpha/monitors/'+sig.monitor_id,{
        method:'GET',
        headers:{'Authorization':'Bearer '+((getActiveKey()||{}).value||''),'x-api-key':((getActiveKey()||{}).value||''),'Content-Type':'application/json'},
        signal:AbortSignal.timeout(8000)
      });
      if(!r.ok) return {sig,ok:false,status:'HTTP '+r.status,lastRun:null};
      const data=await r.json();
      const mStatus=data.status||data.monitor_status||'unknown';
      const lastRun=data.last_run_at||data.last_executed_at||data.updated_at||null;
      const isHealthy=mStatus==='active'||mStatus==='running'||mStatus==='scheduled'||mStatus==='enabled';
      return {sig,ok:isHealthy,status:mStatus,lastRun};
    }catch(e){return {sig,ok:false,status:'unreachable',lastRun:null};}
  }));

  if(btn){btn.textContent='↻ Fetch live status';btn.disabled=false;}
  if(checkedEl) checkedEl.textContent='checked '+istTime(new Date().toISOString());

  const allOk=healthResults.every(r=>r.ok);
  const anyBad=healthResults.some(r=>!r.ok);
  if(dotEl) dotEl.className='dot '+(allOk?'dot-green':anyBad?'dot-red':'dot-amber');
  if(titleEl) titleEl.textContent='Account monitors';
  const sdot=document.getElementById('monitor-health-dot');
  const slbl=document.getElementById('monitor-health-label');
  if(sdot) sdot.className='dot '+(allOk?'dot-green':anyBad?'dot-red':'dot-amber');
  if(slbl) slbl.textContent=allOk?'Monitors: all healthy ✓':(healthResults.filter(r=>!r.ok).length)+' monitor(s) need attention';

  if(!el) return;

  var dmandIds=new Set(DB.signals.map(function(s){return s.monitor_id;}).filter(Boolean));
  var healthMap={};
  healthResults.forEach(function(r){if(r.sig.monitor_id) healthMap[r.sig.monitor_id]=r;});

  var html='';

  html+='<div style="padding:8px 12px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);background:var(--surface2);border-bottom:1px solid var(--border)">Dmand signals — live health</div>';
  if(!healthResults.length){
    html+='<div style="padding:12px;font-size:11px;color:var(--text3)">No active signals.</div>';
  } else {
    html+=healthResults.map(function(r){
      var isOk=r.ok;
      var lastRunStr=r.lastRun?timeSince(r.lastRun):'unknown';
      return '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid var(--border)">'
        +'<span style="font-size:12px;color:'+(isOk?'var(--green)':'var(--red)')+'">'+( isOk?'●':'✗')+'</span>'
        +'<div style="flex:1;min-width:0">'
          +'<div style="font-size:12px;font-weight:600;color:var(--text)">'+esc(r.sig.name)+'</div>'
          +'<div style="display:flex;gap:8px;margin-top:2px">'
            +'<span style="font-size:10px;font-family:var(--mono);padding:1px 5px;border-radius:3px;background:'+(isOk?'rgba(22,163,74,0.08)':'rgba(239,68,68,0.08)')+';color:'+(isOk?'var(--green)':'var(--red)')+';border:1px solid '+(isOk?'rgba(22,163,74,0.2)':'rgba(239,68,68,0.2)')+'">'+esc(r.status)+'</span>'
            +(r.lastRun?'<span style="font-size:10px;font-family:var(--mono);color:var(--text3)">last run: '+lastRunStr+'</span>':'')
            +'<code style="font-size:9px;color:var(--text3)">'+r.sig.monitor_id.slice(0,20)+'...</code>'
          +'</div>'
        +'</div>'
        +'<button class="btn btn-sm" onclick="pollOneSignal(\''+r.sig.id+'\')">Poll</button>'
      +'</div>';
    }).join('');
  }

  var allMonitors=Array.isArray(allRes)?allRes:(allRes&&!allRes.__pFetchError?(allRes.monitors||allRes.data||[]):[]);
  allMonitors=allMonitors.filter(function(m){
    var s=(m.status||'').toLowerCase();
    return s!=='canceled'&&s!=='cancelled'&&s!=='deleted'&&s!=='inactive';
  });
  var byQuery={};
  allMonitors.forEach(function(m){
    var q=(m.query||'').trim();
    if(!byQuery[q]) byQuery[q]=[];
    byQuery[q].push(m);
  });
  var dupeGroups=Object.values(byQuery).filter(function(g){return g.length>1;}).length;
  var dupeIds=new Set();
  Object.values(byQuery).forEach(function(grp){
    if(grp.length>1){
      grp.slice(0,-1).forEach(function(m){ dupeIds.add(m.monitor_id||m.id||''); });
    }
  });

  html+='<div style="padding:8px 12px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);background:var(--surface2);border-bottom:1px solid var(--border);border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center">'
    +'<span>All account monitors ('+allMonitors.length+')</span>'
    +(dupeGroups?'<span style="color:var(--red);font-weight:700">'+dupeGroups+' duplicate group(s)</span>':'<span style="color:var(--green)">no duplicates ✓</span>')
  +'</div>';

  if(allRes&&allRes.__pFetchError){
    html+='<div style="padding:12px;font-size:11px;color:var(--red)">Could not fetch account monitors: HTTP '+allRes.status+'</div>';
  } else if(!allMonitors.length){
    html+='<div style="padding:12px;font-size:11px;color:var(--text3)">No monitors on account.</div>';
  } else {
    html+=allMonitors.map(function(m){
      var mid=m.monitor_id||m.id||'';
      var query=(m.query||'').slice(0,55);
      var inDmand=dmandIds.has(mid);
      var dmandSig=DB.signals.find(function(s){return s.monitor_id===mid;});
      var isDupe=dupeIds.has(mid);
      var health=healthMap[mid];
      return '<div style="display:flex;align-items:center;gap:8px;padding:7px 12px;border-bottom:1px solid var(--border);'+(isDupe?'background:rgba(239,68,68,0.03)':'')+'">'
        +'<div style="flex:1;min-width:0">'
          +'<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:2px">'
            +'<code style="font-size:9px;color:var(--text3)">'+mid.slice(0,22)+'...</code>'
            +(inDmand?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(22,163,74,0.08);color:var(--green);border:1px solid rgba(22,163,74,0.2)">✓ '+esc(dmandSig?dmandSig.name:'')+'</span>':'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:var(--surface3);color:var(--text3);border:1px solid var(--border)">not in Dmand</span>')
            +(isDupe?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(239,68,68,0.1);color:var(--red);border:1px solid rgba(239,68,68,0.2);font-weight:700">DUPLICATE</span>':'')
            +(health?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:'+(health.ok?'rgba(22,163,74,0.08)':'rgba(239,68,68,0.08)')+';color:'+(health.ok?'var(--green)':'var(--red)')+';border:1px solid '+(health.ok?'rgba(22,163,74,0.2)':'rgba(239,68,68,0.2)')+'">'+esc(health.status)+'</span>':'')
          +'</div>'
          +'<div style="font-size:10px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(query)+'...</div>'
        +'</div>'
        +'<button onclick="deleteAccountMonitor(\''+mid+'\')" style="padding:3px 8px;font-size:10px;border-radius:4px;border:1px solid rgba(239,68,68,0.3);background:rgba(239,68,68,0.05);color:var(--red);cursor:pointer;white-space:nowrap;flex-shrink:0">Delete</button>'
      +'</div>';
    }).join('');
  }

  el.innerHTML=html;
}

async function checkMonitorHealth(manual){
  const active = DB.signals.filter(s=>s.status==='active'&&s.monitor_id);
  if(!active.length){
    setMonitorHealthUI([]);
    return;
  }
  if(!getActiveKey()) return;

  const btn=document.getElementById('health-refresh-btn');
  if(btn&&manual){ btn.textContent='Checking...'; btn.disabled=true; }

  const results = await Promise.all(active.map(async function(sig){
    try{
      const r = await fetch(PROXY+'/v1alpha/monitors/'+sig.monitor_id, {
        method:'GET',
        headers:{
          'Authorization':'Bearer '+((getActiveKey()||{}).value||''),
          'x-api-key':((getActiveKey()||{}).value||''),
          'Content-Type':'application/json'
        },
        signal: AbortSignal.timeout(8000)
      });
      if(!r.ok){
        return {sig, ok:false, status:'api_error', httpStatus:r.status, error:'HTTP '+r.status};
      }
      const data = await r.json();
      const mStatus = data.status||data.monitor_status||'unknown';
      const lastRun  = data.last_run_at||data.last_executed_at||data.updated_at||null;
      const nextRun  = data.next_run_at||data.scheduled_at||null;
      const errMsg   = data.error||data.error_message||null;
      const isHealthy = mStatus==='active'||mStatus==='running'||mStatus==='scheduled'||mStatus==='enabled';
      monitorHealthCache[sig.monitor_id] = {
        status:mStatus, last_run:lastRun, next_run:nextRun,
        error:errMsg, checked_at:new Date().toISOString(), healthy:isHealthy, raw:data
      };
      return {sig, ok:isHealthy, status:mStatus, lastRun, nextRun, error:errMsg, raw:data};
    }catch(e){
      return {sig, ok:false, status:'unreachable', error:e.message};
    }
  }));

  setMonitorHealthUI(results);
  if(btn&&manual){ btn.textContent='↻ Refresh'; btn.disabled=false; }

  const allOk  = results.every(r=>r.ok);
  const anyBad = results.some(r=>!r.ok);
  const dot  = document.getElementById('monitor-health-dot');
  const lbl  = document.getElementById('monitor-health-label');
  if(dot){
    dot.className='dot '+(allOk?'dot-green':anyBad?'dot-red':'dot-amber');
  }
  if(lbl){
    const badCount=results.filter(r=>!r.ok).length;
    lbl.textContent=allOk
      ?'Monitors: all healthy ✓'
      :badCount+'/'+active.length+' monitor(s) need attention';
  }

  if(manual){
    const badSigs=results.filter(r=>!r.ok).map(r=>r.sig.name.slice(0,20));
    if(badSigs.length) showAlert('⚠ '+badSigs.length+' monitor issue(s): '+badSigs.join(', '),'warning',6000);
    else showAlert('✓ All '+active.length+' monitors healthy','success',3000);
  }
}

function setMonitorHealthUI(results){
  const panel = document.getElementById('monitor-health-list');
  const titleEl = document.getElementById('health-panel-title');
  const dotEl = document.getElementById('health-panel-dot');
  const checkedEl = document.getElementById('health-panel-checked');
  if(!panel) return;

  if(!results.length){
    panel.innerHTML='<div style="padding:12px 16px;font-size:12px;color:var(--text3)">No active monitors yet.</div>';
    if(titleEl) titleEl.textContent='Monitor health';
    if(dotEl) dotEl.className='dot dot-amber';
    if(checkedEl) checkedEl.textContent='';
    return;
  }

  const allOk=results.every(r=>r.ok);
  const badCount=results.filter(r=>!r.ok).length;
  if(dotEl) dotEl.className='dot '+(allOk?'dot-green':badCount>0?'dot-red':'dot-amber');
  if(titleEl) titleEl.textContent='Monitor health — '+(allOk?'all healthy':badCount+' issue'+(badCount>1?'s':''));
  if(checkedEl) checkedEl.textContent='checked '+istTime(new Date().toISOString());

  panel.innerHTML=results.map(function(r){
    var sig=r.sig;
    var healthy=r.ok;
    var statusColor=healthy?'var(--green)':r.status==='unreachable'?'var(--red)':'var(--amber)';
    var statusIcon=healthy?'●':'✗';
    var lastRunStr=r.lastRun?timeSince(r.lastRun):'never';
    var nextRunStr=r.nextRun?('next: '+istTime(r.nextRun)):'';

    var diagnosis='';
    if(!healthy){
      if(r.status==='unreachable') diagnosis='Cannot reach Parallel API — check proxy/internet';
      else if(r.status==='api_error') diagnosis='API error '+r.httpStatus+' — key may be invalid or monitor deleted';
      else if(r.error) diagnosis=r.error;
      else if(r.status==='paused') diagnosis='Monitor is paused on Parallel — resume it in Parallel dashboard';
      else if(r.status==='error') diagnosis='Parallel reports monitor in error state — try recreating';
      else diagnosis='Status: '+r.status+' — may need recreation';
    }

    return '<div style="display:flex;align-items:center;gap:12px;padding:9px 16px;border-bottom:1px solid var(--border);'+(healthy?'':'background:rgba(239,68,68,0.03)')+'">'
      +'<span style="font-size:14px;color:'+statusColor+';flex-shrink:0">'+statusIcon+'</span>'
      +'<div style="flex:1;min-width:0">'
        +'<div style="display:flex;align-items:center;gap:8px">'
          +'<span style="font-size:12px;font-weight:600;color:var(--text)">'+esc(sig.name)+'</span>'
          +'<span style="font-size:10px;font-family:var(--mono);padding:1px 6px;border-radius:3px;background:'+( healthy?'rgba(22,163,74,0.08)':'rgba(239,68,68,0.08)')+';color:'+statusColor+';border:1px solid '+(healthy?'rgba(22,163,74,0.2)':'rgba(239,68,68,0.2)')+'">'+esc(r.status||'unknown')+'</span>'
          +(r.lastRun?'<span style="font-size:10px;color:var(--text3);font-family:var(--mono)">last run: '+lastRunStr+'</span>':'')
          +(nextRunStr?'<span style="font-size:10px;color:var(--text3);font-family:var(--mono)">'+nextRunStr+'</span>':'')
        +'</div>'
        +(diagnosis?'<div style="font-size:11px;color:var(--red);margin-top:2px">⚠ '+esc(diagnosis)+'</div>':'')
        +'<div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:1px">'+esc(sig.monitor_id||'')+'</div>'
      +'</div>'
      +'<div style="display:flex;gap:5px;flex-shrink:0">'
        +(!healthy?'<button class="btn btn-sm btn-amber" onclick="retrySignal(\''+sig.id+'\')">Recreate</button>':'')
        +'<button class="btn btn-sm" onclick="pollOneSignal(\''+sig.id+'\')">Poll</button>'
      +'</div>'
    +'</div>';
  }).join('');
}

var THEMES = ['light','dark','berserk'];
var THEME_META = {
  light:   { icon:'🌙',  label:'Dark mode',    next:'dark'    },
  dark:    { icon:'⚔️',  label:'Berserk mode', next:'berserk' },
  berserk: { icon:'☀️',  label:'Light mode',   next:'light'   }
};

function cycleTheme(){
  var cur = localStorage.getItem('dmand_theme')||'light';
  var next = THEME_META[cur]?THEME_META[cur].next:'dark';
  applyTheme(next);
}

function applyTheme(theme){
  document.body.classList.remove('dark','berserk');
  if(theme==='dark') document.body.classList.add('dark');
  if(theme==='berserk') document.body.classList.add('berserk');
  localStorage.setItem('dmand_theme', theme);
  var meta = THEME_META[theme]||THEME_META.light;
  var iconEl = document.getElementById('theme-toggle-icon');
  var labelEl = document.getElementById('theme-toggle-label');
  if(iconEl) iconEl.textContent = meta.icon;
  if(labelEl) labelEl.textContent = meta.label;
}

function initTheme(){
  var saved=localStorage.getItem('dmand_theme')||'light';
  applyTheme(saved);
}

function toggleTheme(){ cycleTheme(); }

function showPage(name,el) {
  currentPage = name;
  ['home','signals','events','companies','contacts','campaigns','keys','icp','settings'].forEach(p=>{
    document.getElementById('page-'+p).style.display=p===name?'':'none';
  });
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  if(el) el.classList.add('active');
  const titles={home:'Home',signals:'Signals',events:'Events',companies:'Companies',contacts:'Contacts',campaigns:'Campaigns',keys:'API Keys',icp:'ICP Personas',settings:'Settings'};
  document.getElementById('page-title').textContent=titles[name]||name;
  if(name==='home') renderHome();
  if(name==='events') renderEvents();
  if(name==='companies') renderCompanies();
  if(name==='contacts') renderContacts();
  if(name==='signals') fetchAllMonitorHealth();
  if(name==='keys'){ renderKeys(); loadOAIKeyUI(); loadFullenrichKeyUI(); loadCrustdataKeyUI(); loadAutoboundKeysUI(); loadSmartleadKeyUI(); updateUsageUI(); }
  if(name==='icp'){ renderPersonas(); loadOutreachSettingsUI(); loadQualifierPromptUI(); populateIcpSignalDropdown(); }
  if(name==='campaigns'){ renderCampaignList(); loadCampFormDefaults(); }
  if(name==='settings') loadSettingsUI();
}

let alertTimer=null;
function showAlert(msg,type='info',dur=4000){
  clearTimeout(alertTimer);
  document.getElementById('alert-area').innerHTML=`<div class="alert alert-${type}">${msg}</div>`;
  if(dur>0) alertTimer=setTimeout(()=>{document.getElementById('alert-area').innerHTML='';},dur);
}

function getActiveKey(){
  const live=DB.keys.filter(k=>!k.exhausted);
  if(!live.length) return null;
  if(DB.meta.activeKeyId){
    const explicit=live.find(k=>k.id===DB.meta.activeKeyId);
    if(explicit) return explicit;
  }
  DB.meta.activeKeyId=live[0].id;
  save();
  return live[0];
}

function setActiveKey(id){
  const k=DB.keys.find(k=>k.id===id);
  if(!k||k.exhausted){showAlert('Cannot set exhausted key as active.','error');return;}
  DB.meta.activeKeyId=id;
  save(); renderKeys();
  showAlert('Active key set to "'+k.label+'". Go to Signals → ↺ Recreate monitors to switch.','success',8000);
  log('Active key changed to: '+k.label,'blue');
}

async function rotateKey(){
  log('Key rotation: attempting to switch to next available key','amber');
  const l=DB.keys.filter(k=>!k.exhausted);
  if(!l.length){showAlert('All API keys exhausted. Add a new key in Keys tab.','error',0);return false;}
  const curIdx=l.findIndex(k=>k.id===DB.meta.activeKeyId);
  const nextIdx=(curIdx+1)%l.length;
  DB.meta.activeKeyId=l[nextIdx].id;
  log('Key rotated → '+l[nextIdx].label,'blue');
  showAlert('Key rotated to "'+l[nextIdx].label+'". Re-creating monitors...','warning',5000);
  await recreateAllMonitors(); save(); renderKeys(); return true;
}

async function listAccountMonitors(){
  const key=getActiveKey();
  if(!key){showAlert('No active Parallel key.','error');return;}
  const btn=document.getElementById('btn-list-monitors');
  const el=document.getElementById('account-monitors-list');
  if(!el) return;
  if(btn){btn.textContent='Fetching...';btn.disabled=true;}
  el.innerHTML='<div style="display:flex;align-items:center;gap:6px;color:var(--text3)"><div class="spinner" style="width:10px;height:10px"></div>Loading monitors from Parallel...</div>';

  const res=await pFetch('GET','/v1alpha/monitors');
  if(btn){btn.textContent='↻ Fetch all monitors';btn.disabled=false;}

  if(!res||res.__pFetchError){
    el.innerHTML='<span style="color:var(--red)">Failed to fetch monitors: '+(res?'HTTP '+res.status:'no response')+'</span>';
    return;
  }

  var monitors=Array.isArray(res)?res:(res.monitors||res.data||[]);
  if(!monitors.length){
    el.innerHTML='<span style="color:var(--text3)">No monitors found on this account.</span>';
    return;
  }

  var dmandIds=new Set(DB.signals.map(function(s){return s.monitor_id;}).filter(Boolean));
  var queryMap={};
  DB.signals.forEach(function(s){if(s.monitor_id) queryMap[s.monitor_id]=s.name;});

  var byQuery={};
  monitors.forEach(function(m){
    var q=(m.query||m.monitor_query||'').slice(0,60);
    if(!byQuery[q]) byQuery[q]=[];
    byQuery[q].push(m);
  });

  var html='<div style="margin-bottom:8px;font-size:11px;color:var(--text2)">'
    +monitors.length+' monitor(s) on account · '
    +monitors.filter(function(m){return dmandIds.has(m.monitor_id||m.id);}).length+' linked to Dmand · '
    +'<span style="color:var(--red);font-weight:600">'
    +Object.values(byQuery).filter(function(g){return g.length>1;}).length+' duplicate group(s)'
    +'</span></div>';

  html+='<div style="display:flex;flex-direction:column;gap:4px">';
  monitors.forEach(function(m){
    var mid=m.monitor_id||m.id||'';
    var query=(m.query||m.monitor_query||'').slice(0,55);
    var status=m.status||m.cadence||'';
    var inDmand=dmandIds.has(mid);
    var dmandName=queryMap[mid]||'';
    var isDupe=dupeIds&&dupeIds.has(mid);

    html+='<div style="display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:6px;border:1px solid '+(isDupe?'rgba(239,68,68,0.25)':inDmand?'rgba(22,163,74,0.2)':'var(--border)')+';background:'+(isDupe?'rgba(239,68,68,0.04)':inDmand?'rgba(22,163,74,0.03)':'var(--surface2)')+';">'
      +'<div style="flex:1;min-width:0">'
        +'<div style="display:flex;align-items:center;gap:6px;margin-bottom:2px">'
          +'<code style="font-size:9px;color:var(--text3)">'+mid.slice(0,24)+'...</code>'
          +(inDmand?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(22,163,74,0.1);color:var(--green);border:1px solid rgba(22,163,74,0.2)">✓ '+esc(dmandName)+'</span>':'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:var(--surface3);color:var(--text3);border:1px solid var(--border)">not in Dmand</span>')
          +(isDupe?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(239,68,68,0.1);color:var(--red);border:1px solid rgba(239,68,68,0.2);font-weight:700">DUPLICATE</span>':'')
        +'</div>'
        +'<div style="font-size:10px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(query)+'...</div>'
      +'</div>'
      +'<button onclick="deleteAccountMonitor(\''+mid+'\')" style="padding:3px 8px;font-size:10px;border-radius:4px;border:1px solid rgba(239,68,68,0.3);background:rgba(239,68,68,0.05);color:var(--red);cursor:pointer;white-space:nowrap;flex-shrink:0">Delete</button>'
    +'</div>';
  });
  html+='</div>';
  el.innerHTML=html;
}

async function deleteAccountMonitor(mid){
  if(!mid||mid==='undefined'){showAlert('Invalid monitor ID','error');return;}
  var btns=document.querySelectorAll('[onclick*="'+mid+'"]');
  btns.forEach(function(b){b.disabled=true;b.textContent='Deleting...';b.style.opacity='0.5';});
  log('Deleting monitor: '+mid+'...','gray');
  const res=await pFetch('DELETE','/v1alpha/monitors/'+mid);
  const failed = res&&res.__pFetchError;
  if(failed){
    showAlert('Delete failed: HTTP '+res.status+' — '+(res.body||'').slice(0,80),'error');
    log('Delete FAILED for '+mid+': HTTP '+res.status+' '+(res.body||'').slice(0,100),'red');
    btns.forEach(function(b){b.disabled=false;b.textContent='Delete';b.style.opacity='1';});
    return;
  }
  log('Deleted monitor: '+mid,'green');
  showAlert('Monitor deleted ✓','success',3000);
  var sig=DB.signals.find(function(s){return s.monitor_id===mid;});
  if(sig){ sig.monitor_id=null; sig.status='error'; save(); renderSignals(); }
  setTimeout(fetchAllMonitorHealth, 2000);
}

async function testParallelKey(id){
  const k=DB.keys.find(k=>k.id===id);
  if(!k) return;
  log('Testing key: "'+k.value.slice(0,8)+'...'+k.value.slice(-4)+'" ('+k.value.length+' chars)','blue');
  try{
    const r=await fetch(PROXY+'/v1alpha/monitors',{
      method:'GET',
      headers:{
        'Authorization':'Bearer '+k.value,
        'x-api-key':k.value,
        'Content-Type':'application/json'
      },
      signal:AbortSignal.timeout(10000)
    });
    const txt=await r.text();
    if(r.ok){
      log('Key OK! HTTP '+r.status+' — '+txt.slice(0,150),'green');
      showAlert('Key is valid!','success');
    } else {
      log('Key FAILED: HTTP '+r.status+' — '+txt.slice(0,200),'red');
      showAlert('Key invalid: '+txt.slice(0,100),'error');
    }
  }catch(e){
    log('Test error: '+e.message,'red');
  }
}

async function addKey(){
  const val=document.getElementById('new-key').value.trim();
  const label=document.getElementById('new-key-label').value.trim()||'Parallel AI Key';
  if(!val){showAlert('Enter an API key.','error');return;}
  const newKey={id:'key_'+Date.now(),value:val,label,spent:0,exhausted:false,balance:null,added_at:new Date().toISOString()};
  DB.keys=[newKey];
  DB.meta.activeKeyId=newKey.id;
  document.getElementById('new-key').value='';
  document.getElementById('new-key-label').value='';
  save(); renderKeys(); updateMetrics(); updateKeySidebarDot();
  showAlert('Key saved. Go to Signals → ↺ Recreate monitors to activate.','success',8000);
  log('Key updated: '+label,'blue');
  checkKeyConnection();
}

async function removeKey(id){
  DB.keys=[];
  DB.meta.activeKeyId=null;
  save(); renderKeys(); updateMetrics();
  showAlert('Key removed.','info');
}

async function checkKeyConnection(){
  const k=DB.keys[0];
  if(!k){showAlert('No key saved.','error');return;}
  log('Checking connection for "'+k.label+'"...','gray');
  try{
    const r=await fetch(PROXY+'/v1alpha/monitors',{
      method:'GET',
      headers:{'Authorization':'Bearer '+k.value,'x-api-key':k.value,'Content-Type':'application/json'},
      signal:AbortSignal.timeout(10000)
    });
    const txt=await r.text();
    log('Connection test: HTTP '+r.status+' '+txt.slice(0,100),'gray');
    if(r.ok){
      k.exhausted=false; save(); renderKeys();
      showAlert('✓ Connected to Parallel AI','success',4000);
      log('Key "'+k.label+'" connected ✓','green');
    } else if(r.status===402){
      k.exhausted=true; save(); renderKeys();
      showAlert('⚠ Credits exhausted (402) — replace your key in the Replace key section.','error',0);
      log('Key "'+k.label+'" EXHAUSTED — credits used up (402)','red');
    } else if(r.status===401||r.status===403){
      showAlert('✗ Invalid key — authentication failed ('+r.status+')','error');
      log('Key invalid: HTTP '+r.status,'red');
    } else {
      showAlert('HTTP '+r.status+' from Parallel — check Activity log','warning');
      log('Connection: HTTP '+r.status,'amber');
    }
  }catch(e){
    showAlert('Could not reach Parallel AI — check proxy connection','error');
    log('Connection error: '+e.message,'red');
  }
}

async function checkKeyBalance(){
  const k=DB.keys[0];
  if(!k){showAlert('No key saved.','error');return;}
  const bal=await fetchKeyBalance(k);
  if(bal!==null){
    k.balance=parseFloat(bal);
    save(); renderKeys();
    initUsage(); DB.usage.parBalance=bal; updateUsageUI();
    showAlert('Balance: $'+k.balance.toFixed(2),'success',4000);
  } else {
    showAlert('Could not fetch balance — check Activity log','warning');
  }
}

async function autoDetectAllKeys(){
  if(!DB.keys.length){showAlert('No keys to detect.','info');return;}
  showAlert('Checking all keys...','info',0);
  log('Auto-detecting status for all '+DB.keys.length+' key(s)...','blue');
  for(var i=0;i<DB.keys.length;i++){
    await autoDetectKeyStatus(DB.keys[i].id);
    if(i<DB.keys.length-1) await new Promise(function(r){setTimeout(r,500);});
  }
  showAlert('All keys checked — see Activity log for details.','success',5000);
}

async function markKeyExhausted(id){
  const k=DB.keys.find(k=>k.id===id);
  if(!k) return;
  k.exhausted=true;
  showAlert('"'+k.label+'" marked exhausted. Rotating...','warning');
  await rotateKey(); renderKeys(); updateMetrics();
}

function setKeyStatus(id, status){
  var k=DB.keys.find(function(k){return k.id===id;});
  if(!k) return;
  if(status==='active'){
    k.exhausted=false;
    DB.meta.activeKeyId=id;
    log('Key "'+k.label+'" set as Active','blue');
    showAlert('"'+k.label+'" is now the active key. Go to Signals → ↺ Recreate monitors.','success',8000);
  } else if(status==='standby'){
    k.exhausted=false;
    if(DB.meta.activeKeyId===id){
      var others=DB.keys.filter(function(x){return x.id!==id&&!x.exhausted;});
      DB.meta.activeKeyId=others.length?others[0].id:null;
    }
    log('Key "'+k.label+'" set to Standby','gray');
  } else if(status==='exhausted'){
    k.exhausted=true;
    if(DB.meta.activeKeyId===id){
      var others=DB.keys.filter(function(x){return x.id!==id&&!x.exhausted;});
      DB.meta.activeKeyId=others.length?others[0].id:null;
      if(others.length) showAlert('"'+k.label+'" exhausted. Auto-switched to "'+others[0].label+'".','warning',6000);
    }
    log('Key "'+k.label+'" marked as Exhausted','red');
  }
  save(); renderKeys(); updateMetrics();
}

async function autoDetectKeyStatus(id){
  var k=DB.keys.find(function(k){return k.id===id;});
  if(!k){return;}
  log('Auto-detecting status for key "'+k.label+'"...','gray');
  try{
    var r=await fetch(PROXY+'/v1alpha/monitors',{
      method:'GET',
      headers:{'Authorization':'Bearer '+k.value,'x-api-key':k.value,'Content-Type':'application/json'},
      signal:AbortSignal.timeout(10000)
    });
    var txt=await r.text();
    log('Key "'+k.label+'" test: HTTP '+r.status+' '+txt.slice(0,100),'gray');
    if(r.ok){
      var bal=await fetchKeyBalance(k);
      if(bal!==null){
        k.balance=parseFloat(bal);
        if(k.balance<=0){
          k.exhausted=true;
          log('Key "'+k.label+'" has $0 balance — auto-marking exhausted','red');
          showAlert('"'+k.label+'" is out of credits ($0). Marked as exhausted.','error',6000);
          save(); renderKeys(); updateMetrics(); return;
        }
      }
      if(k.exhausted){ k.exhausted=false; }
      var balStr=bal!=null?(' · $'+parseFloat(bal).toFixed(2)+' balance'):'';
      log('Key "'+k.label+'" is ACTIVE ✓'+balStr,'green');
      showAlert('"'+k.label+'" is working ✓'+balStr,'success',4000);
    } else if(r.status===402||r.status===429||(txt.toLowerCase().includes('credit')||txt.toLowerCase().includes('quota')||txt.toLowerCase().includes('limit')||txt.toLowerCase().includes('exhaust'))){
      k.exhausted=true;
      log('Key "'+k.label+'" is EXHAUSTED (credits/quota exceeded)','red');
      showAlert('"'+k.label+'" is exhausted — credits used up. Marked as exhausted.','error',6000);
    } else if(r.status===401||r.status===403){
      k.exhausted=true;
      log('Key "'+k.label+'" is INVALID (auth failed)','red');
      showAlert('"'+k.label+'" is invalid — authentication failed.','error',6000);
    } else {
      log('Key "'+k.label+'" returned HTTP '+r.status+' — check manually','amber');
      showAlert('"'+k.label+'" returned HTTP '+r.status+'. Check Activity log.','warning');
    }
  }catch(e){
    log('Key "'+k.label+'" test error: '+e.message,'red');
    showAlert('Could not reach Parallel API — check proxy connection.','error');
  }
  save(); renderKeys(); updateMetrics();
}

let modalCompanyDomain = null;
let modalCompanyName   = null;
let modalCompanyId     = null;  // Crustdata company_id if available

function openFindPeople(companyDomain, companyName){
  var apolloKey=DB.settings&&DB.settings.apolloKey;
  if(!apolloKey){ showAlert('Add Apollo API key in Keys tab first.','error'); return; }

  modalCompanyDomain = companyDomain;
  modalCompanyName   = companyName;

  var company=DB.companies.find(function(co){return co.domain===companyDomain;});
  var signalIds=[];
  if(company&&company.event_ids&&company.event_ids.length){
    company.event_ids.forEach(function(eid){
      var ev=DB.events.find(function(e){return e.event_id===eid;});
      if(ev&&ev.signal_id&&signalIds.indexOf(ev.signal_id)<0) signalIds.push(ev.signal_id);
    });
  }

  var icpTargets=[];
  signalIds.forEach(function(sid){
    var sig=DB.signals.find(function(s){return s.id===sid;});
    if(sig&&sig.icp_targets&&sig.icp_targets.length){
      sig.icp_targets.forEach(function(t){
        var persona=DB.personas.find(function(p){return p.id===t.persona_id;});
        if(persona&&!icpTargets.find(function(x){return x.persona_id===t.persona_id;})){
          icpTargets.push({persona_id:t.persona_id,persona_name:t.persona_name||persona.name,max_contacts:t.max_contacts||5,signal_id:sid,campaign_id:t.campaign_id,hr_campaign_id:t.hr_campaign_id});
        }
      });
    }
  });

  if(!icpTargets.length&&DB.personas&&DB.personas.length){
    icpTargets=DB.personas.map(function(p){return {persona_id:p.id,persona_name:p.name,max_contacts:5,signal_id:null};});
  }

  document.getElementById('modal-company-context').textContent = companyName + '  ·  ' + companyDomain;

  var sel = document.getElementById('modal-persona-sel');
  sel.innerHTML = icpTargets.map(function(t){
    var existing=DB.contacts.filter(function(c){return c.domain===companyDomain&&c.icp_persona_id===t.persona_id;}).length;
    var label=t.persona_name+(existing?' ('+existing+' found)':'')+(t.max_contacts?' · max '+t.max_contacts:'');
    return '<option value="'+t.persona_id+'" data-max="'+t.max_contacts+'" data-sid="'+(t.signal_id||'')+'">'+esc(label)+'</option>';
  }).join('');

  if(icpTargets.length===1) sel.value=icpTargets[0].persona_id;
  onModalPersonaChange();

  modalEventId = null;
  if(company&&company.event_ids&&company.event_ids.length){
    var latestEv=null;
    company.event_ids.forEach(function(eid){
      var ev=DB.events.find(function(e){return e.event_id===eid&&e.type==='event';});
      if(ev&&(!latestEv||new Date(ev.fetched_at||0)>new Date(latestEv.fetched_at||0))) latestEv=ev;
    });
    if(latestEv) modalEventId=latestEv.event_id;
  }

  document.getElementById('modal-status').textContent = '';
  document.getElementById('modal-status').style.color = 'var(--text3)';
  document.getElementById('modal-find-btn').disabled = false;
  document.getElementById('modal-spinner').style.display = 'none';

  document.getElementById('find-people-modal').classList.remove('hidden');
}

function onModalPersonaChange(){
  var sel = document.getElementById('modal-persona-sel');
  var p = DB.personas.find(function(x){ return x.id === sel.value; });
  var hint = document.getElementById('modal-persona-hint');
  if(!p){ hint.textContent = ''; return; }
  var parts = [];
  if(p.titles) parts.push('Title: ' + p.titles);
  if(p.dept)   parts.push('Function: ' + p.dept);
  if(p.seniority && p.seniority !== 'any') parts.push('Seniority: ' + p.seniority);
  if(p.location) parts.push('Country: ' + p.location);
  hint.textContent = parts.join('  |  ');
}

function closeModal(){
  document.getElementById('find-people-modal').classList.add('hidden');
  modalCompanyDomain = null;
  modalCompanyName   = null;
}

async function runFindPeople(){
  var sel=document.getElementById('modal-persona-sel');
  var personaId=sel.value;
  var selectedOpt=sel.options[sel.selectedIndex];
  var limit=Math.min(25,Math.max(1,parseInt((selectedOpt&&selectedOpt.dataset.max)||document.getElementById('modal-limit').value)||10));
  var statusEl=document.getElementById('modal-status');
  function setStatus(msg,color){statusEl.textContent=msg;statusEl.style.color=color||'var(--text3)';}

  if(!personaId){setStatus('Select a persona.','var(--amber)');return;}
  var persona=DB.personas.find(function(p){return p.id===personaId;});
  if(!persona) return;

  var apolloKey=DB.settings&&DB.settings.apolloKey;
  if(!apolloKey){setStatus('Add Apollo API key in Keys tab first.','var(--red)');return;}

  var btn=document.getElementById('modal-find-btn');
  var spinner=document.getElementById('modal-spinner');
  btn.disabled=true; spinner.style.display='inline-block';
  setStatus('Searching Apollo...','var(--blue)');

  try{
    var companyDomain=modalCompanyDomain;
    var companyName=modalCompanyName;

    var params={};
    if(companyDomain) params['q_organization_domains_list[]']=[companyDomain];

    if(persona.seniority&&persona.seniority!=='any'){
      var _senMap2={'Owner / Partner':'owner','CXO':'c_suite','Vice President':'vp',
        'Experienced Manager':'manager','Strategic':'senior','Entry Level Manager':'entry',
        'Founder':'founder','Owner':'owner','C-Suite':'c_suite','VP':'vp',
        'Manager':'manager','Senior':'senior','Entry':'entry','Director':'director','Head':'head',
        'founder':'founder','owner':'owner','c_suite':'c_suite','partner':'partner',
        'vp':'vp','head':'head','director':'director','manager':'manager',
        'senior':'senior','entry':'entry','intern':'intern'};
      var senArr=persona.seniority.split(',').map(function(s){
        return _senMap2[s.trim()]||s.trim();
      }).filter(Boolean).filter(function(v,i,a){return a.indexOf(v)===i;});
      if(senArr.length) params['person_seniorities[]']=senArr;
    }
    if(persona.titles&&persona.titles.trim()){
      var _titles=persona.titles?(persona.titles.split('|').map(function(t){return t.trim();}).filter(Boolean)):[];
    if(_titles.length) params['person_titles[]']=_titles;
    }
    if(persona.similar_titles==='false') params['include_similar_titles']='false';
    if(persona.location&&persona.location.trim()) params['person_locations[]']=[persona.location.trim()];
    if(persona.org_location&&persona.org_location.trim()) params['organization_locations[]']=[persona.org_location.trim()];
    if(persona.email_status&&persona.email_status.trim()){
      var _es=persona.email_status?(persona.email_status.split(',').map(function(s){return s.trim();}).filter(Boolean)):[];
    if(_es.length) params['contact_email_status[]']=_es;
    }
    if(persona.headcount&&persona.headcount.trim()){
      var _hc=persona.headcount?(persona.headcount.split('|').map(function(s){return s.trim();}).filter(Boolean)):[];
    if(_hc.length) params['organization_num_employees_ranges[]']=_hc;
    }
    if(persona.revenue_min&&parseInt(persona.revenue_min)) params['revenue_range[min]']=parseInt(persona.revenue_min);
    if(persona.revenue_max&&parseInt(persona.revenue_max)) params['revenue_range[max]']=parseInt(persona.revenue_max);
    if(persona.org_job_titles&&persona.org_job_titles.trim()){
      var _ot=persona.org_job_titles?(persona.org_job_titles.split('|').map(function(t){return t.trim();}).filter(Boolean)):[];
    if(_ot.length) params['q_organization_job_titles[]']=_ot;
    }
    if(persona.min_jobs&&parseInt(persona.min_jobs)) params['organization_num_jobs_range[min]']=parseInt(persona.min_jobs);
    if(persona.max_jobs&&parseInt(persona.max_jobs)) params['organization_num_jobs_range[max]']=parseInt(persona.max_jobs);
    if(persona.tech_any&&persona.tech_any.trim()){
      var _ta=persona.tech_any?(persona.tech_any.split('|').map(function(t){return t.trim();}).filter(Boolean)):[];
    if(_ta.length) params['currently_using_any_of_technology_uids[]']=_ta;
    }
    if(persona.tech_all&&persona.tech_all.trim()){
      var _tall=persona.tech_all?(persona.tech_all.split('|').map(function(t){return t.trim();}).filter(Boolean)):[];
    if(_tall.length) params['currently_using_all_of_technology_uids[]']=_tall;
    }
    if(persona.tech_none&&persona.tech_none.trim()){
      var _tn=persona.tech_none?(persona.tech_none.split('|').map(function(t){return t.trim();}).filter(Boolean)):[];
    if(_tn.length) params['currently_not_using_any_of_technology_uids[]']=_tn;
    }
    if(persona.keyword&&persona.keyword.trim()) params['q_keywords']=persona.keyword.trim();
    params['per_page']=limit; params['page']=1;

    function buildQS(p){
      var parts=[];
      Object.keys(p).forEach(function(k){
        var v=p[k];
        if(Array.isArray(v)) v.forEach(function(item){parts.push(encodeURIComponent(k)+'='+encodeURIComponent(item));});
        else parts.push(encodeURIComponent(k)+'='+encodeURIComponent(v));
      });
      return parts.join('&');
    }

    log('Apollo Find People: '+companyDomain+' | seniority='+JSON.stringify(params['person_seniorities[]']||[]),'blue');

    var profiles=[];
    var apolloDirectUrl='https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(params);
    var r;
    try{
      r=await fetch(apolloDirectUrl,{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
    }catch(directErr){
      log('Apollo direct failed ('+directErr.message+') — trying via proxy','amber');
      r=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(params),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
    }
    var txt=await r.text();
    log('Apollo [full filters] HTTP '+r.status,(r.ok?'green':'red'));

    if(r.ok){
      var d=JSON.parse(txt);
      profiles=(d.people||d.contacts||[]);
    } else if(r.status===403){
      setStatus('Apollo 403: Master API key required','var(--red)');
      log('Apollo 403: Master API key required for People Search','red');
      btn.disabled=false; spinner.style.display='none'; return;
    } else if(r.status===530||r.status===503||r.status===524||r.status===522){
      setStatus('Apollo 530 — retrying in 3s...','var(--amber)');
      log('Apollo '+r.status+': network timeout — retrying once...','amber');
      await new Promise(function(res){setTimeout(res,3000);});
      var rR=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(params),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
      if(rR.ok){ var dR=JSON.parse(await rR.text()); profiles=(dR.people||dR.contacts||[]); }
      else { setStatus('Apollo unavailable — try again later','var(--red)'); btn.disabled=false; spinner.style.display='none'; return; }
    } else {
      log('Apollo error: HTTP '+r.status+' '+txt.slice(0,150),'red');
    }

    if(!profiles.length&&Object.keys(params).length>3){
      log('No results — retrying seniority+domain only','amber');
      setStatus('Retrying...','var(--amber)');
      var p2={'q_organization_domains_list[]':[companyDomain],'per_page':limit,'page':1};
      if(params['person_seniorities[]']) p2['person_seniorities[]']=params['person_seniorities[]'];
      var r2=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(p2),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
      if(r2.ok){ var d2=JSON.parse(await r2.text()); profiles=(d2.people||d2.contacts||[]); }
    }

    btn.disabled=false; spinner.style.display='none';

    if(!profiles.length){
      setStatus('No profiles found','var(--amber)');
      log('Apollo: 0 profiles for "'+companyName+'"','amber');
      return;
    }
    setStatus(profiles.length+' found — adding...','var(--green)');

    var ev=null;
    if(modalEventId) ev=DB.events.find(function(e){return e.event_id===modalEventId;});
    var sig=null;
    if(ev) sig=DB.signals.find(function(s){return s.id===ev.signal_id;});

    var added=0,updated=0;
    profiles.slice(0,limit).forEach(function(p){
      var name=p.name||((p.first_name||'')+' '+(p.last_name||'')).trim()||'';
      var title=p.title||'';
      var linkedin=p.linkedin_url||'';
      if(linkedin&&!linkedin.startsWith('http')) linkedin='https://www.linkedin.com/in/'+linkedin;
      var location=[p.city,p.state,p.country].filter(Boolean).join(', ');
      var seniority=p.seniority||'';
      var department=(p.departments&&p.departments[0])||'';
      var apolloId=p.id||'';
      var orgName=(p.organization&&p.organization.name)||companyName;

      if(!name&&!linkedin) return;

      var domLow=(companyDomain||'').toLowerCase();
      var dedup=linkedin
        ?DB.contacts.find(function(c){return c.linkedin&&c.linkedin===linkedin;})
        :DB.contacts.find(function(c){return c.name===name&&(c.domain||'').toLowerCase()===domLow;});

      if(dedup){
        if(title) dedup.title=title;
        if(seniority&&!dedup.seniority) dedup.seniority=seniority;
        if(apolloId&&!dedup.apollo_id) dedup.apollo_id=apolloId;
        if(sig){dedup.signal_id=sig.id;dedup.signal_name=sig.name;}
        updated++;
        return;
      }

      var newContact={
        id:'con_'+Date.now()+'_'+Math.random().toString(36).slice(2,7),
        name,title,headline:title,seniority,department,linkedin,
        domain:domLow,company:orgName,location,
        apollo_id:apolloId,source:'apollo',source_type:'manual',
        persona_id:persona.id,persona_name:persona.name,
        icp_persona_id:persona.id,icp_persona_name:persona.name,
        signal_id:sig?sig.id:'',signal_name:sig?sig.name:'',
        event_brief:ev?(ev.output||'').slice(0,300):'',
        event_date:ev?ev.event_date:'',
        found_at:new Date().toISOString()
      };
      DB.contacts.push(newContact);
      added++;

      if(linkedin||apolloId){
        setTimeout((function(cid){return function(){enrichSingleEmail(cid);};})(newContact.id),added*3000);
      } else if(linkedin){
        setTimeout((function(cid){return function(){
          var _c=DB.contacts.find(function(x){return x.id===cid;});
          if(_c&&getActiveAutoboundKey()&&!_c.autobound_insights) triggerInsightsForContact(_c);
        };})(newContact.id), added*3000+1000);
      }
    });

    save(); renderContacts(false);
    var nbEl=document.getElementById('nb-contacts');
    if(nbEl) nbEl.textContent=DB.contacts.length;
    setStatus(added+' added'+(updated?' · '+updated+' updated':''),added?'var(--green)':'var(--text3)');
    log('Apollo Find People: '+added+' added, '+updated+' updated for "'+companyName+'"','green');

  }catch(e){
    btn.disabled=false; spinner.style.display='none';
    setStatus('Error: '+e.message,'var(--red)');
    log('Apollo find people error: '+e.message,'red');
  }
}

async function pFetch(method, path, body){
  const key=getActiveKey();
  if(!key){ log('No active Parallel API key','red'); return null; }
  const freshKey=DB.keys.find(function(k){return k.id===key.id;});
  const keyVal=(freshKey||key).value;
  const opts={
    method:method,
    headers:{
      'Authorization':'Bearer '+keyVal,
      'x-api-key':keyVal,
      'Content-Type':'application/json',
      'Accept':'application/json'
    },
    signal:AbortSignal.timeout(25000)
  };
  if(body&&method!=='GET'&&method!=='DELETE') opts.body=JSON.stringify(body);
  try{
    const r=await fetch(PROXY+path, opts);
    const txt=await r.text();
  var _snippet=path.includes('/events?')?'':' '+txt.slice(0,100);
  log('pFetch '+method+' '+path+': HTTP '+r.status+_snippet,'gray');
    if(!r.ok){
      log('Parallel API error '+r.status+': '+txt.slice(0,300),'red');
      if(r.status===402){
        log('⚠ CREDITS EXHAUSTED — Parallel API returned 402. Replace your key.','red');
        if(DB.keys[0]){ DB.keys[0].exhausted=true; save(); renderKeys(); updateKeySidebarDot(); }
        showAlert('⚠ Parallel credits exhausted (402). Replace your API key in API Keys tab.','error',0);
      }
      return {__pFetchError:true, status:r.status, body:txt};
    }
    if(!txt||!txt.trim()) return {__pFetchOk:true, status:r.status};
    try{ return JSON.parse(txt); } catch(e){ return txt; }
  }catch(e){
    log('pFetch network error '+path+': '+e.message,'red');
    return {__pFetchError:true, status:0, body:e.message};
  }
}

async function createMonitorOnParallel(sig){
  const key=getActiveKey(); if(!key) return null;
  const payload={
    query: sig.query,
    frequency: sig.frequency,
    metadata: {signal_id:sig.id, signal_name:sig.name},
    structured_output: {
      type: 'object',
      properties: {
        company_name:        { type: 'string', description: 'Name of the company mentioned in the signal. Extract if present, leave empty if not clear.' },
        company_domain:      { type: 'string', description: 'Primary website domain of the company e.g. acme.com without https://. Leave empty if unknown.' },
        company_linkedin_url:{ type: 'string', description: 'LinkedIn company page URL e.g. https://www.linkedin.com/company/acme. Leave empty if not found.' }
      }
    }
  };
  const res=await pFetch('POST','/v1alpha/monitors', payload);
  if(res&&res.__pFetchError){
    log('Monitor creation HTTP '+res.status+' for "'+sig.name+'": '+(res.body||'').slice(0,200),'red');
    return null;
  }
  if(res&&res.monitor_id){
    log('Monitor created: '+sig.name.slice(0,25)+' → '+res.monitor_id.slice(0,16)+'...','green');
    sig.monitor_id=res.monitor_id; sig.key_label=key.label; sig.status='active';
    return res.monitor_id;
  }
  log('Monitor creation failed for "'+sig.name+'": '+(res?JSON.stringify(res).slice(0,100):'null'),'red');
  return null;
}
async function deleteMonitorOnParallel(mid){
  if(!mid) return;
  const res=await pFetch('DELETE','/v1alpha/monitors/'+mid);
  if(res&&res.__pFetchError) log('Could not delete monitor '+mid.slice(0,16)+': HTTP '+res.status,'amber');
  else log('Deleted old monitor: '+mid.slice(0,16)+'...','gray');
}
async function recreateAllMonitors(){
  const active=DB.signals.filter(function(s){return s.status==='active'||s.status==='error';});
  if(!active.length){showAlert('No signals to recreate.','info');return;}
  if(!getActiveKey()){showAlert('Add a new Parallel API key first.','error');return;}
  if(!confirm('Recreate '+active.length+' monitor(s) on Parallel?\n\nOld monitors will be deleted first, then new ones created with the fixed schema.')) return;
  showAlert('Recreating '+active.length+' monitors...','info',0);
  let ok=0,fail=0;
  for(var i=0;i<active.length;i++){
    var sig=active[i];
    if(sig.monitor_id){
      await deleteMonitorOnParallel(sig.monitor_id);
      await new Promise(r=>setTimeout(r,400)); // small delay between delete and create
    }
    sig.monitor_id=null; sig.status='pending';
    renderSignals();
    const mid=await createMonitorOnParallel(sig);
    if(mid){ sig.status='active'; ok++; }
    else { sig.status='error'; fail++; }
    renderSignals();
    if(i<active.length-1) await new Promise(r=>setTimeout(r,300));
  }
  DB.meta.schemaFixed = true;
  save(); renderSignals();
  showAlert('Done: '+ok+' recreated'+(fail?' · '+fail+' failed':'')+'. Doing catch-up poll with 14d lookback...','success', 10000);

  if(ok > 0){
    const savedLookback = DB.settings.lookback;
    DB.settings.lookback = '14d';
    log('Running catch-up poll with 14d lookback...','blue');
    await new Promise(r=>setTimeout(r,3000));
    await pollAllNow();
    DB.settings.lookback = savedLookback;
    log('Catch-up poll complete. Lookback restored to '+savedLookback,'blue');
  }
}

async function createSignal(){
  const name=document.getElementById('sig-name').value.trim();
  const query=document.getElementById('sig-query').value.trim();
  const freq=document.getElementById('sig-freq').value;
  const cat=document.getElementById('sig-cat').value;
  const notes=document.getElementById('sig-notes').value.trim();
  if(!name){showAlert('Enter a signal name.','error');return;}
  if(!query){showAlert('Enter a monitor query.','error');return;}
  if(!getActiveKey()){showAlert('Add a Parallel API key first (Keys tab).','error');return;}
  var icpTargets=[];
  document.querySelectorAll('.sig-icp-row').forEach(function(row){
    var sel=row.querySelector('.sig-icp-persona-sel');
    var inp=row.querySelector('.sig-icp-max-inp');
    var campSel=row.querySelector('.sig-icp-campaign-sel');
    var hrSel=row.querySelector('.sig-icp-hr-sel');
    if(sel&&sel.value&&inp){
      var persona=DB.personas.find(function(p){return p.id===sel.value;});
      var campId=campSel&&campSel.value||'';
      var campName=campSel&&campSel.selectedOptions&&campSel.selectedOptions[0]?campSel.selectedOptions[0].text:'';
      var hrId=hrSel&&hrSel.value||'';
      var hrName=hrSel&&hrSel.selectedOptions&&hrSel.selectedOptions[0]?hrSel.selectedOptions[0].text:'';
      var hrCamp=hrId?(DB.hrCampaigns||[]).find(function(c){return String(c.hr_id)===String(hrId);}):null;
      if(persona) icpTargets.push({
        persona_id:persona.id,
        persona_name:persona.name,
        max_contacts:parseInt(inp.value)||5,
        campaign_id:campId||null,
        campaign_name:campId?campName:null,
        hr_campaign_id:hrId||null,
        hr_campaign_name:hrId?hrName:null,
        hr_mode:hrCamp?hrCamp.hr_mode:'signal'
      });
    }
  });
  const sig={id:'sig_'+Date.now(),name,query,frequency:freq,category:cat,notes,status:'pending',monitor_id:null,key_label:null,created_at:new Date().toISOString(),icp_targets:icpTargets};
  DB.signals.push(sig);
  showAlert('Creating monitor on Parallel...','info',0);
  const mid=await createMonitorOnParallel(sig);
  if(mid){
    showAlert('Signal "'+name+'" created. Monitor ID: '+mid,'success',8000);
    document.getElementById('sig-name').value='';
    document.getElementById('sig-query').value='';
    document.getElementById('sig-notes').value='';
    document.getElementById('sig-icp-targets').innerHTML='';
  } else {
    sig.status='error';
    showAlert('Monitor creation failed. Signal saved — use Retry once proxy is confirmed running.','error',8000);
  }
  save(); renderSignals(); updateMetrics();
}

function addSignalICPTarget(){
  var container=document.getElementById('sig-icp-targets');
  if(!container) return;
  if(!DB.personas||!DB.personas.length){showAlert('Create ICP personas first (ICP tab).','warning');return;}
  var row=document.createElement('div');
  row.className='sig-icp-row';
  row.style.cssText='display:flex;align-items:center;gap:8px;padding:8px 10px;background:var(--surface2);border:1px solid var(--border);border-radius:6px;flex-wrap:wrap';
  var opts=DB.personas.map(function(p){return '<option value="'+p.id+'">'+p.name+'</option>';}).join('');
  var assignedCamps=(DB.campaigns||[]).filter(function(c){return c.mode;});
  var campOpts='<option value="">No email campaign</option>'+assignedCamps.map(function(c){
    return '<option value="'+c.id+'">'+c.name+'</option>';
  }).join('');
  var hrCamps=(DB.hrCampaigns||[]).filter(function(c){return c.hr_status==='ACTIVE'||c.hr_status==='FINISHED';});
  var hrOpts='<option value="">No LinkedIn campaign</option>'+hrCamps.map(function(c){
    return '<option value="'+c.hr_id+'">'+c.hr_name+'</option>';
  }).join('');
  row.innerHTML='<select class="sig-icp-persona-sel form-select" style="flex:1;min-width:120px;font-size:12px">'+opts+'</select>'
    +'<div style="display:flex;align-items:center;gap:4px;flex-shrink:0">'
      +'<label style="font-size:11px;color:var(--text3);white-space:nowrap">Max:</label>'
      +'<input type="number" class="sig-icp-max-inp form-input" value="5" min="1" max="50" style="width:55px;font-size:12px">'
    +'</div>'
    +'<select class="sig-icp-campaign-sel form-select" style="flex:1;min-width:140px;font-size:12px" title="📧 Smartlead email campaign">'+campOpts+'</select>'
    +'<select class="sig-icp-hr-sel form-select" style="flex:1;min-width:160px;font-size:12px;border-color:rgba(10,102,194,0.3);color:#0a66c2" title="💼 HeyReach LinkedIn campaign">'+hrOpts+'</select>'
    +'<button onclick="this.parentElement.remove()" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;padding:0 4px;flex-shrink:0">×</button>';
  container.appendChild(row);
  fetchCampaignsForIcpRow(row);
}

async function fetchCampaignsForIcpRow(row){
  var sel=row.querySelector('.sig-icp-campaign-sel');
  if(!sel) return;
  const slKey=DB.outreach&&DB.outreach.smartleadKey;
  if(!slKey){
    var cached=(DB.campaigns||[]).filter(function(c){return c.mode;});
    sel.innerHTML='<option value="">No email campaign</option>'+cached.map(function(c){
      return '<option value="'+c.id+'">'+c.name+'</option>';
    }).join('');
    return;
  }
  try{
    const r=await fetch(PROXY+'/smartlead/api/v1/campaigns?api_key='+encodeURIComponent(slKey),{signal:AbortSignal.timeout(8000)});
    if(!r.ok) return;
    const data=await r.json();
    const allCamps=Array.isArray(data)?data:(data.data||[]);
    if(allCamps.length) DB.campaigns=DB.campaigns.map(function(dc){
      var fresh=allCamps.find(function(ac){return String(ac.id)===String(dc.id);});
      return fresh?Object.assign({},dc,{name:fresh.title||fresh.name||dc.name,status:fresh.status||dc.status}):dc;
    });
    var assigned=(DB.campaigns||[]).filter(function(c){return c.mode;});
    sel.innerHTML='<option value="">No email campaign</option>'+assigned.map(function(c){
      return '<option value="'+c.id+'">'+c.name+'</option>';
    }).join('');
    if(!assigned.length) sel.innerHTML='<option value="">— No assigned campaigns (set in Campaigns tab) —</option>';
  }catch(e){ log('Could not fetch campaigns for ICP row: '+e.message,'gray'); }
}

function oneClickPushSmartlead(contactId, btnEl){
  var c=DB.contacts.find(function(x){return x.id===contactId;});
  if(!c){showAlert('Contact not found.','error');return;}
  if(!c.business_email||c.business_email==='N/A'){showAlert('No email for '+c.name+' — enrich first.','warning');return;}
  var sig=c.signal_id?DB.signals.find(function(s){return s.id===c.signal_id;}):null;
  var target=null;
  if(sig&&sig.icp_targets){
    target=sig.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;});
    if(!target&&sig.icp_targets.length) target=sig.icp_targets[0];
  }
  if(!target||!target.campaign_id){
    showAlert('No Smartlead campaign assigned to this signal ICP. Set one in Campaigns tab.','warning',5000);
    return;
  }
  openEmailModalWithCampaign(contactId, target.campaign_id);
}

function oneClickPushHeyreach(contactId, btnEl){
  var c=DB.contacts.find(function(x){return x.id===contactId;});
  if(!c){showAlert('Contact not found.','error');return;}
  if(!c.linkedin){showAlert('No LinkedIn URL for '+c.name,'warning');return;}
  var sig=c.signal_id?DB.signals.find(function(s){return s.id===c.signal_id;}):null;
  var target=null;
  if(sig&&sig.icp_targets){
    target=sig.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;});
    if(!target&&sig.icp_targets.length) target=sig.icp_targets[0];
  }
  if(!target||!target.hr_campaign_id){
    showAlert('No HeyReach campaign assigned to this signal ICP. Set one in Campaigns tab.','warning',5000);
    return;
  }
  openLiModalWithCampaign(contactId, target.hr_campaign_id);
}

async function autoGenerateAndPushContact(contact){
  if(contact.smartlead_campaign_id&&contact.heyreach_campaign_id){
    log('Auto-push: '+contact.name+' already in both campaigns — skipping','gray'); return;
  }
  log('── Auto-push: '+contact.name+' ['+( contact.source_type||'?')+'] | signal: '+(contact.signal_name||'?')+' | email='+(contact.business_email&&contact.business_email!=='N/A'?contact.business_email:'none')+' | linkedin='+(contact.linkedin?'✓':'✗'),'blue');

  var sig=DB.signals.find(function(s){return s.id===contact.signal_id;});
  if(!sig||!sig.icp_targets||!sig.icp_targets.length){
    log('Auto-push: ✗ no ICP targets on signal "'+(sig?sig.name:'NOT FOUND')+'"','red'); return;
  }
  var target=sig.icp_targets.find(function(t){
    return t.persona_id===contact.icp_persona_id||t.persona_name===contact.icp_persona_name;
  });
  if(!target){
    log('Auto-push: ✗ no matching ICP target for '+contact.name,'amber'); return;
  }

  const slKey=DB.outreach&&DB.outreach.smartleadKey;
  if(slKey&&target.campaign_id){
    if(String(contact.smartlead_campaign_id)===String(target.campaign_id)){
      log('Auto-push: '+contact.name+' already in SL campaign','gray');
    } else {
      const email=contact.business_email&&contact.business_email!=='N/A'?contact.business_email:'';
      if(!email){
        log('Auto-push: no email for '+contact.name+' — skipping Smartlead','gray');
      } else {
        const alreadyInCampaign=await checkContactInSmartleadCampaign(email, target.campaign_id);
        if(alreadyInCampaign){
          contact.smartlead_campaign_id=target.campaign_id;
          contact.smartlead_campaign_name=target.campaign_name;
          save(); renderContacts(false);
        } else {
          var camp=DB.campaigns&&DB.campaigns.find(function(c){return String(c.id)===String(target.campaign_id);});
          var stepCount=(camp&&camp.sequences&&camp.sequences.length)||3;
          var mapping=camp&&camp.var_mapping||{};
          var hasMappings=Object.keys(mapping).length>0;
          function slField(ourVar,def){
            if(hasMappings&&mapping[ourVar]) return mapping[ourVar];
            if(camp&&camp.custom_vars){
              var match=camp.custom_vars.find(function(v){return v.toLowerCase()===ourVar.toLowerCase();});
              if(match) return match;
            }
            return def;
          }
          var mode=camp&&camp.mode||'signal';
          var steps=await generateEmailStepsForContact(contact, sig, camp, stepCount, mode);
          if(steps){
            var customFields={};
            for(var si=1;si<=stepCount;si++){
              var sk='step'+si; var sd=steps[sk];
              if(!sd) continue;
              if(sd.subject) customFields[slField('subject_'+si,'subject_'+si)]=sd.subject;
              customFields[slField('body_'+si,'body_'+si)]=(sd.body||'').replace(/<[^>]+>/g,' ').replace(/  +/g,' ').trim();
            }
            customFields.job_title=contact.title||'';
            customFields.linkedin_url=contact.linkedin||'';
            try{
              var nameParts=(contact.name||'').split(' ');
              var slPayload={lead_list:[{email:email,first_name:nameParts[0]||'',last_name:nameParts.slice(1).join(' ')||'',company_name:contact.company||'',custom_fields:customFields}],settings:{ignore_global_block_list:false,ignore_unsubscribe_list:false,ignore_community_bounce_list:false}};
              var r=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+target.campaign_id+'/leads?api_key='+encodeURIComponent(slKey),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(slPayload),signal:AbortSignal.timeout(15000)});
              var d=await r.json();
              if(d.ok||d.message||d.added_count>=0||d.status==='success'){
                contact.smartlead_campaign_id=target.campaign_id;
                contact.smartlead_campaign_name=target.campaign_name;
                contact.smartlead_sent='auto';
                contact.smartlead_pushed_at=new Date().toISOString();
                log('── Smartlead ✓: '+contact.name+' → "'+target.campaign_name+'" | steps: '+stepCount+' | mode: '+mode,'green');
                showAlert('✓ '+contact.name+' pushed to Smartlead "'+target.campaign_name+'"','success',4000);
              } else {
                log('Smartlead: ✗ push failed for '+contact.name+': '+JSON.stringify(d).slice(0,150),'red');
              }
            }catch(e){ log('Smartlead push error: '+e.message,'red'); }
          }
        }
      }
    }
  } else if(!slKey) {
    log('Auto-push: no Smartlead key — skipping email push','amber');
  }

  if(target.hr_campaign_id && contact.linkedin){
    if(String(contact.heyreach_campaign_id)===String(target.hr_campaign_id)){
      log('Auto-push: '+contact.name+' already in HeyReach campaign','gray');
    } else {
      var hrMode=target.hr_mode||'signal';
      var liMsgs=contact.linkedin_messages&&contact.linkedin_messages.CM
        ? contact.linkedin_messages
        : await generateLinkedInMessagesForContact(contact, sig, hrMode);
      if(liMsgs){
        await pushToHeyreach(contact, target.hr_campaign_id, target.hr_campaign_name, liMsgs);
      }
    }
  } else if(target.hr_campaign_id && !contact.linkedin){
    log('Auto-push: no LinkedIn URL for '+contact.name+' — skipping HeyReach','amber');
  }

  save(); renderContacts(false);
}

async function checkContactInSmartleadCampaign(email, campaignId){
  const slKey=DB.outreach&&DB.outreach.smartleadKey;
  try{
    const r=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+campaignId+'/leads?api_key='+encodeURIComponent(slKey)+'&limit=1&offset=0&email='+encodeURIComponent(email),{
      signal:AbortSignal.timeout(8000)
    });
    if(!r.ok) return false;
    const d=await r.json();
    const leads=Array.isArray(d)?d:(d.data||d.leads||[]);
    return leads.some(function(l){ return (l.email||'').toLowerCase()===email.toLowerCase(); });
  }catch(e){ return false; }
}

async function generateEmailStepsForContact(contact, sig, camp, stepCount, mode){
  const oaiKey=DB.settings&&DB.settings.oaiKey;
  if(!oaiKey){ log('Auto-push: no OpenAI key','gray'); return null; }

  if(!mode||mode==='manual') mode='signal';
  var o = DB.outreach || {};

  var exampleGuide='';
  if(camp&&camp.example_steps&&camp.example_steps.length){
    exampleGuide='\n=== EXAMPLE EMAILS (mirror tone, length, structure exactly) ===\n';
    camp.example_steps.forEach(function(ex,i){
      if(!ex||(!ex.subject&&!ex.body)) return;
      exampleGuide+='--- Step '+(i+1)+' ---\n';
      if(ex.subject) exampleGuide+='Subject: '+ex.subject+'\n';
      if(ex.body)    exampleGuide+='Body:\n'+ex.body+'\n';
      exampleGuide+='\n';
    });
    exampleGuide+='=== END EXAMPLES ===\n';
  }

  var stepInstructions='';
  if(camp&&camp.example_steps&&camp.example_steps.length){
    stepInstructions='\n=== PER-STEP CTA RULES ===\n';
    camp.example_steps.forEach(function(ex,i){
      stepInstructions+='Step '+(i+1)+': '+(ex&&ex.cta?ex.cta:'No specific CTA — keep conversational')+'. NO links. Plain text only.\n';
    });
    stepInstructions+='=== END RULES ===\n';
  }

  var signalContext='';
  var ibContext='';

  if(mode==='signal'||mode==='icebreaker'){
    signalContext='\n=== SIGNAL CONTEXT (use to open or reference naturally) ===\n'
      +'Signal: '+(sig.name||'')+'.\n'
      +'Event: '+(contact.event_brief||'').slice(0,300)+(contact.event_date?' ('+contact.event_date+')':'')+'.\n'
      +'=== END SIGNAL ===\n';
  }

  if(mode==='icebreaker'){
    var insights=buildInsightsSummary(contact);
    if(insights){
      ibContext='\n=== PERSONALISATION INSIGHTS (use 1-2 max, weave in naturally) ===\n'
        +insights
        +'\n=== END INSIGHTS ===\n';
    }
  }

  var outreachContext='';
  if(o.companyBrief||o.valueProp||o.painPoints){
    outreachContext='\n=== YOUR COMPANY ===\n'
      +(o.companyBrief?'Brief: '+o.companyBrief+'\n':'')
      +(o.valueProp?'Value prop: '+o.valueProp+'\n':'')
      +(o.painPoints?'Pain points solved: '+o.painPoints+'\n':'')
      +'=== END COMPANY ===\n';
  }

  var prospectLine='Prospect: '+(contact.name||'')+', '+(contact.title||'')+' at '+(contact.company||'')+'.\n';

  var stepSchema='{"step1":{"subject":"...","body":"..."}';
  for(var si=2;si<=stepCount;si++) stepSchema+=',"step'+si+'":{"body":"..."}';
  stepSchema+='}';

  var modeLabel={signal:'Signal-only',icebreaker:'Signal+IB'}[mode]||'Signal-only';
  log('Email gen ['+modeLabel+']: '+stepCount+' steps for '+contact.name+' at '+contact.company,'gray');

  const systemPrompt='You write cold B2B email sequences. Return ONLY valid JSON matching: '+stepSchema
    +'\nRules:\n1. No signature, no sign-off, no name at end — platform adds those automatically.\n'
    +'2. Plain text only — NO links, NO URLs, NO HTML, NO markdown.\n'
    +'3. Keep emails short and human — 3-5 sentences per step max.\n'
    +'4. Mirror the tone and length from the example emails exactly.';

  const userPrompt=prospectLine
    +outreachContext
    +signalContext
    +ibContext
    +exampleGuide
    +stepInstructions
    +'\nWrite a '+stepCount+'-step cold email sequence. '
    +'Step 1: subject + body. Steps 2+: body only (add subject only if starting a new thread). '
    +'Follow per-step CTAs. Plain text — no links.';

  try{
    const res=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Authorization':'Bearer '+oaiKey,'Content-Type':'application/json'},
      body:JSON.stringify({model:DB.settings.oaiModel||'gpt-4o-mini',messages:[{role:'system',content:systemPrompt},{role:'user',content:userPrompt}],max_tokens:2000}),
      signal:AbortSignal.timeout(30000)
    });
    const d=await res.json();
    const txt=(d.choices&&d.choices[0]&&d.choices[0].message&&d.choices[0].message.content)||'';
    const clean=txt.replace(/```json|```/g,'').trim();
    const parsed=JSON.parse(clean);
    log('Email gen: ✓ ['+modeLabel+'] '+stepCount+' steps for '+contact.name,'green');
    return parsed;
  }catch(e){ log('Email gen: ✗ failed for '+contact.name+' — '+e.message,'red'); return null; }
}

function normalizeSeniorityForCrustdata(senArr){
  var map={
    'Founder':        'Owner / Partner',
    'Owner':          'Owner / Partner',
    'Owner / Partner':'Owner / Partner',
    'CXO':            'CXO',
    'C-Suite':        'CXO',
    'Vice President': 'Vice President',
    'VP':             'Vice President',
    'Director':       'Director',
    'Manager':        'Experienced Manager',
    'Experienced Manager':'Experienced Manager',
    'Senior':         'Strategic',
    'Strategic':      'Strategic',
    'Senior IC':      'Strategic',
    'Entry':          'Entry Level Manager',
    'Entry Level':    'Entry Level Manager',
    'Entry Level Manager':'Entry Level Manager',
  };
  var result=[];
  senArr.forEach(function(s){
    var mapped=map[s.trim()]||s.trim();
    if(result.indexOf(mapped)===-1) result.push(mapped);
  });
  return result;
}

async function autoEnrichContactsForEvent(ev){
  log('autoEnrichContacts: starting for event '+ev.event_id.slice(0,16)+' signal_id='+ev.signal_id,'gray');
  var sig=DB.signals.find(function(s){return s.id===ev.signal_id;});
  log('autoEnrichContacts: signal='+(sig?sig.name:'NOT FOUND')+' icp_targets='+(sig?JSON.stringify(sig.icp_targets||[]).slice(0,80):'none'),'gray');
  if(!sig||!sig.icp_targets||!sig.icp_targets.length){
    log('No ICP targets on signal "'+( sig?sig.name:'?')+'" — skipping auto-contact enrichment','gray');
    return;
  }
  var apolloKey=DB.settings&&DB.settings.apolloKey;
  log('autoEnrichContacts: apolloKey='+(apolloKey?'set ('+apolloKey.slice(0,6)+'...)':'MISSING'),'gray');
  if(!apolloKey){log('No Apollo key — cannot auto-enrich contacts. Add it in Keys tab.','red');return;}

  var domain=ev.superEnrichment&&ev.superEnrichment.data&&ev.superEnrichment.data.domain;
  var companyName=ev.superEnrichment&&ev.superEnrichment.data&&ev.superEnrichment.data.company_name;
  log('autoEnrichContacts: domain='+domain+' company='+companyName,'gray');
  if(!domain){log('No domain from Super Enrich — skipping auto-contact enrichment','red');return;}

  log('══ Contact enrichment pipeline: '+companyName+' ('+domain+') | '+sig.icp_targets.length+' ICP persona(s) | signal: '+sig.name,'blue');

  var totalAdded=0;
  for(var ti=0;ti<sig.icp_targets.length;ti++){
    var target=sig.icp_targets[ti];
    var persona=DB.personas.find(function(p){return p.id===target.persona_id;});
    if(!persona&&target.persona_name){
      persona=DB.personas.find(function(p){return p.name.toLowerCase()===target.persona_name.toLowerCase();});
      if(persona){
        log('Persona ID mismatch — matched by name: "'+persona.name+'" (updating ICP target)','amber');
        target.persona_id=persona.id; // fix stale ID for future runs
        save();
      }
    }
    if(!persona){
      log('⚠ Persona "'+target.persona_name+'" not found — re-add ICP target on signal "'+sig.name+'" (Signals → Edit)','red');
      showAlert('ICP persona missing on "'+sig.name+'" — go to Signals → ✏ Edit to re-add it.','warning',8000);
      continue;
    }

    log('Searching: '+persona.name+' at '+domain+' (max '+target.max_contacts+')','blue');
    try{
      var added=await autoFindContactsForPersona(domain,companyName,persona,target.max_contacts,sig,ev);
      totalAdded+=added;
      log('── Apollo found: '+added+' new contact(s) for "'+persona.name+'" at '+domain+(added>0?' | enriching emails next...':''),'green');
    }catch(e){
      log('Auto-contact error ['+persona.name+']: '+e.message,'red');
    }
    if(ti<sig.icp_targets.length-1) await new Promise(function(r){setTimeout(r,1500);});
  }

  if(totalAdded>0){
    save();
    var nbEl=document.getElementById('nb-contacts');
    if(nbEl) nbEl.textContent=DB.contacts.length;
    showAlert('Auto-enriched: '+totalAdded+' contact(s) added from "'+sig.name+'"','success');
  } else {
    log('Auto-contact enrichment: no new contacts found for '+domain,'gray');
  }
}

async function autoFindContactsForPersona(domain,companyName,persona,maxContacts,sig,ev){
  var apolloKey=DB.settings&&DB.settings.apolloKey;
  if(!apolloKey){
    log('No Apollo API key — add it in Keys tab','red');
    showAlert('Add Apollo API key in Keys tab to find contacts.','error');
    return 0;
  }
  var eventDate=ev.event_date||(new Date().toISOString().split('T')[0]);
  var eventBrief=(ev.output||'').replace(/\s+/g,' ').trim().slice(0,300);

  var params={};

  if(domain) params['q_organization_domains_list[]']=[domain];

  if(persona.seniority&&persona.seniority!=='any'){
    var _senMap={'Owner / Partner':'owner','CXO':'c_suite','Vice President':'vp',
      'Experienced Manager':'manager','Strategic':'senior','Entry Level Manager':'entry',
      'Founder':'founder','Owner':'owner','C-Suite':'c_suite','VP':'vp',
      'Manager':'manager','Senior':'senior','Entry':'entry','Director':'director','Head':'head',
      'founder':'founder','owner':'owner','c_suite':'c_suite','partner':'partner',
      'vp':'vp','head':'head','director':'director','manager':'manager',
      'senior':'senior','entry':'entry','intern':'intern'};
    var senArr=persona.seniority.split(',').map(function(s){
      return _senMap[s.trim()]||s.trim();
    }).filter(Boolean).filter(function(v,i,a){return a.indexOf(v)===i;});
    if(senArr.length) params['person_seniorities[]']=senArr;
  }

  if(persona.titles&&persona.titles.trim()){
    params['person_titles[]']=persona.titles.split('|').map(function(t){return t.trim();}).filter(Boolean);
  }

  if(persona.similar_titles==='false') params['include_similar_titles']='false';

  if(persona.location&&persona.location.trim()){
    if(persona.location&&persona.location.trim()) params['person_locations[]']=[persona.location.trim()];
  }

  if(persona.org_location&&persona.org_location.trim()){
    if(persona.org_location&&persona.org_location.trim()) params['organization_locations[]']=[persona.org_location.trim()];
  }

  if(persona.email_status&&persona.email_status.trim()){
    params['contact_email_status[]']=persona.email_status.split(',').map(function(s){return s.trim();}).filter(Boolean);
  }

  if(persona.headcount&&persona.headcount.trim()){
    params['organization_num_employees_ranges[]']=persona.headcount.split('|').map(function(s){return s.trim();}).filter(Boolean);
  }

  if(persona.revenue_min&&parseInt(persona.revenue_min)) params['revenue_range[min]']=parseInt(persona.revenue_min);
  if(persona.revenue_max&&parseInt(persona.revenue_max)) params['revenue_range[max]']=parseInt(persona.revenue_max);

  if(persona.org_job_titles&&persona.org_job_titles.trim()){
    params['q_organization_job_titles[]']=persona.org_job_titles.split('|').map(function(t){return t.trim();}).filter(Boolean);
  }

  if(persona.min_jobs&&parseInt(persona.min_jobs)) params['organization_num_jobs_range[min]']=parseInt(persona.min_jobs);
  if(persona.max_jobs&&parseInt(persona.max_jobs)) params['organization_num_jobs_range[max]']=parseInt(persona.max_jobs);

  if(persona.tech_any&&persona.tech_any.trim()){
    params['currently_using_any_of_technology_uids[]']=persona.tech_any.split('|').map(function(t){return t.trim();}).filter(Boolean);
  }
  if(persona.tech_all&&persona.tech_all.trim()){
    params['currently_using_all_of_technology_uids[]']=persona.tech_all.split('|').map(function(t){return t.trim();}).filter(Boolean);
  }
  if(persona.tech_none&&persona.tech_none.trim()){
    params['currently_not_using_any_of_technology_uids[]']=persona.tech_none.split('|').map(function(t){return t.trim();}).filter(Boolean);
  }

  if(persona.keyword&&persona.keyword.trim()) params['q_keywords']=persona.keyword.trim();

  params['per_page']=Math.min(maxContacts,25);
  params['page']=1;

  function buildQS(p){
    var parts=[];
    Object.keys(p).forEach(function(k){
      var v=p[k];
      if(Array.isArray(v)) v.forEach(function(item){parts.push(encodeURIComponent(k)+'='+encodeURIComponent(item));});
      else parts.push(encodeURIComponent(k)+'='+encodeURIComponent(v));
    });
    return parts.join('&');
  }

  log('Apollo people search: '+persona.name+' at '+domain,'blue');
  var _ps='seniority:'+JSON.stringify(params['person_seniorities[]']||[])
      +' | titles:'+(params['person_titles[]']?params['person_titles[]'].length+'t':'any')
      +' | hc:'+(params['organization_num_employees_ranges[]']?JSON.stringify(params['organization_num_employees_ranges[]']):'any')
      +' | loc:'+(params['person_locations[]']?params['person_locations[]'][0]:'any')
      +' | max:'+params['per_page'];
    log('── Apollo search: '+companyDomain+' | '+_ps,'gray');

  var profiles=[];
  try{
    var r=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(params),{
      method:'POST',
      headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
      body:'{}',
      signal:AbortSignal.timeout(20000)
    });
    var txt=await r.text();
    log('Apollo [full filters]: HTTP '+r.status,(r.ok?'green':'red'));

    if(r.ok){
      var d=JSON.parse(txt);
      profiles=(d.people||d.contacts||[]);
      log('Apollo [full filters]: '+profiles.length+' profile(s) found','green');
    } else if(r.status===403){
      log('Apollo 403: Master API key required for People Search','red');
      showAlert('Apollo People Search needs a Master API key. Check Keys tab.','error',8000);
      return 0;
    } else if(r.status===530||r.status===503||r.status===524||r.status===522||r.status===0){
      log('Apollo '+r.status+': network timeout — retrying in 3s...','amber');
      await new Promise(function(res){setTimeout(res,3000);});
      var rRetry=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(params),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
      if(rRetry.ok){
        var dRetry=JSON.parse(await rRetry.text());
        profiles=(dRetry.people||dRetry.contacts||[]);
        log('Apollo [retry]: '+profiles.length+' profile(s)','green');
      } else {
        log('Apollo [retry] still failed: HTTP '+rRetry.status+' — skipping','red');
      }
    } else {
      log('Apollo error [full]: HTTP '+r.status+' '+txt.slice(0,150),'red');
    }

    if(!profiles.length&&(params['person_titles[]']||params['organization_num_employees_ranges[]']||params['currently_using_any_of_technology_uids[]'])){
      log('Apollo: retrying with seniority+domain only','amber');
      var p2={};
      if(params['q_organization_domains_list[]']) p2['q_organization_domains_list[]']=params['q_organization_domains_list[]'];
      if(params['person_seniorities[]']) p2['person_seniorities[]']=params['person_seniorities[]'];
      if(params['organization_locations[]']) p2['organization_locations[]']=params['organization_locations[]'];
      p2['per_page']=Math.min(maxContacts,25); p2['page']=1;
      var r2=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(p2),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
      if(r2.ok){
        var d2=JSON.parse(await r2.text());
        profiles=(d2.people||d2.contacts||[]);
        log('Apollo [seniority+domain]: '+profiles.length+' profile(s)','green');
      }
    }

    if(!profiles.length){
      log('Apollo: retrying with domain only','amber');
      var p3={'q_organization_domains_list[]':[domain],'per_page':Math.min(maxContacts,25),'page':1};
      var r3=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/mixed_people/api_search?'+buildQS(p3),{
        method:'POST',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
        body:'{}',signal:AbortSignal.timeout(20000)
      });
      if(r3.ok){
        var d3=JSON.parse(await r3.text());
        profiles=(d3.people||d3.contacts||[]);
        var _s3count=profiles.length;
        log('Apollo [domain only]: '+_s3count+' profile(s)'+(profiles.length===0?' — try manual search or different domain':''),'green');
      }
    }
  }catch(e){
    log('Apollo people search error: '+e.message,'red');
    return 0;
  }

  if(!profiles.length){
    log('Apollo: 0 profiles for "'+companyName+'" ('+domain+') — company may not be in Apollo database','amber');
    return 0;
  }

  domain=domain.toLowerCase();
  var added=0;

  for(var pi=0;pi<Math.min(profiles.length,maxContacts);pi++){
    var p=profiles[pi];
    var name=p.name||((p.first_name||'')+' '+(p.last_name||'')).trim()||'';
    var title=p.title||p.headline||'';
    var linkedin=p.linkedin_url||'';
    if(linkedin&&!linkedin.startsWith('http')) linkedin='https://www.linkedin.com/in/'+linkedin;
    var location=[p.city,p.state,p.country].filter(Boolean).join(', ');
    var seniority=p.seniority||'';
    var department=(p.departments&&p.departments[0])||'';
    var apolloId=p.id||'';
    var orgName=(p.organization&&p.organization.name)||companyName;

    if(!name&&!linkedin) continue;

    var domLow=domain.toLowerCase();
    var dedup=linkedin
      ?DB.contacts.find(function(c){return c.linkedin&&c.linkedin===linkedin;})
      :DB.contacts.find(function(c){return c.name===name&&(c.domain||'').toLowerCase()===domLow;});

    if(dedup){
      dedup.signal_name=sig.name; dedup.signal_id=sig.id;
      dedup.event_brief=eventBrief; dedup.event_date=eventDate;
      dedup.source_type='automated';
      if(title) dedup.title=title;
      if(seniority&&!dedup.seniority) dedup.seniority=seniority;
      if(apolloId&&!dedup.apollo_id) dedup.apollo_id=apolloId;
      var hasEmail=dedup.business_email&&dedup.business_email!=='N/A';
      if(!hasEmail&&(dedup.linkedin||dedup.apollo_id)&&(DB.settings.apolloKey||DB.settings.fullenrichKey)&&dedup.email_status!=='enriching'&&dedup.email_status!=='done'){
        dedup.email_status=null;
        setTimeout((function(cid){return function(){enrichSingleEmail(cid);};})(dedup.id),2000);
      }
      continue;
    }

    var newContact={
      id:'con_'+Date.now()+'_'+Math.random().toString(36).slice(2,7),
      signal_id:sig.id,signal_name:sig.name,
      event_id:ev?ev.event_id:'',
      event_brief:eventBrief,event_date:eventDate,
      name,title,headline:title,seniority,department,linkedin,
      domain:domLow,company:orgName,location,
      apollo_id:apolloId,
      source:'apollo',source_type:'automated',
      persona_id:persona.id,persona_name:persona.name,
      icp_persona_id:persona.id,icp_persona_name:persona.name,
      found_at:new Date().toISOString()
    };
    DB.contacts.push(newContact);
    added++;

    if(linkedin||apolloId){
      setTimeout((function(cid){return function(){enrichSingleEmail(cid);};})(newContact.id),added*3000);
    } else if(linkedin){
      setTimeout((function(cid){return function(){
        var _c=DB.contacts.find(function(x){return x.id===cid;});
        if(_c&&getActiveAutoboundKey()&&!_c.autobound_insights) triggerInsightsForContact(_c);
      };})(newContact.id), added*3000+1000);
    }
  }

  return added;
}

function clearErrorSignals(){
  var errCount=DB.signals.filter(function(s){return s.status==='error';}).length;
  if(!errCount){showAlert('No error signals to clear.','info');return;}
  if(!confirm('Remove '+errCount+' error signal(s) from Dmand? (Monitors were never created so nothing to delete on Parallel)')) return;
  DB.signals=DB.signals.filter(function(s){return s.status!=='error';});
  save(); renderSignals(); updateMetrics();
  showAlert('Cleared '+errCount+' error signal(s).','success');
}

async function deleteSignal(id){
  const sig=DB.signals.find(s=>s.id===id);
  if(!sig||!confirm(`Delete signal "${sig.name}"?`)) return;
  if(sig.monitor_id) await deleteMonitorOnParallel(sig.monitor_id);
  DB.signals=DB.signals.filter(s=>s.id!==id);
  DB.events=DB.events.filter(e=>e.signal_id!==id);
  saveAndSync(); renderSignals(); updateMetrics();
}

async function retrySignal(id){
  const sig=DB.signals.find(s=>s.id===id); if(!sig) return;
  showAlert('Retrying...','info',0);
  if(sig.monitor_id){
    log('retrySignal: deleting old monitor '+sig.monitor_id.slice(0,16)+'... before recreating','gray');
    await deleteMonitorOnParallel(sig.monitor_id);
    sig.monitor_id=null;
    sig.status='pending';
    save(); renderSignals();
  }
  const mid=await createMonitorOnParallel(sig);
  if(mid) showAlert('✓ Monitor created: '+mid,'success');
  else showAlert('Still failing. Check API key and proxy.','error');
  save(); renderSignals();
}

async function pollSignal(sig){
  if(!sig.monitor_id||sig.status!=='active') return {real:0,completions:0,errors:0,skipped:true};
  const lookback=DB.settings.lookback||'3d';
  const url=`/v1alpha/monitors/${sig.monitor_id}/events?lookback=${lookback}`;

  let res=null;
  for(var attempt=0; attempt<2; attempt++){
    res=await pFetch('GET',url);

    if(res&&res.__pFetchError){
      const httpStatus=res.status;
      const errBody=(res.body||'').slice(0,200);

      if(httpStatus===404){
        log('Monitor DELETED on Parallel for "'+sig.name+'" (404) — auto-recreating...','red');
        sig.monitor_id=null; sig.status='pending'; renderSignals();
        const mid=await createMonitorOnParallel(sig);
        if(mid){
          sig.status='active';
          log('Monitor recreated for "'+sig.name+'" → '+mid.slice(0,16)+'...','green');
          showAlert('Monitor auto-recreated for "'+sig.name+'"','warning',6000);
          save(); renderSignals();
        } else {
          sig.status='error';
          log('Failed to recreate monitor for "'+sig.name+'"','red');
          save(); renderSignals();
        }
        return {real:0,completions:0,errors:1,reason:'monitor_deleted'};
      }

      if(httpStatus===401||httpStatus===403){
        log('Key invalid/expired for "'+sig.name+'" ('+httpStatus+') — rotating key...','red');
        await rotateKey();
        return {real:0,completions:0,errors:1,reason:'key_invalid'};
      }

      if(httpStatus===500||httpStatus===502||httpStatus===503||httpStatus===0){
        if(attempt===0){
          log('Server error ('+httpStatus+') for "'+sig.name+'" — retrying in 2s...','amber');
          await new Promise(r=>setTimeout(r,2000));
          continue;
        }
        log('Poll FAILED "'+sig.name+'" — HTTP '+httpStatus+': '+errBody,'red');
        return {real:0,completions:0,errors:1,reason:'server_error_'+httpStatus};
      }

      log('Poll error "'+sig.name+'" — HTTP '+httpStatus+': '+errBody,'red');
      return {real:0,completions:0,errors:1,reason:'http_'+httpStatus};
    }

    if(res!==null) break; // success
    await new Promise(r=>setTimeout(r,1500));
  }

  if(res===null||res===undefined){
    log('Poll FAILED "'+sig.name+'" — null response after retry','red');
    return {real:0,completions:0,errors:1,reason:'null_response'};
  }

  let rawEvents=[];
  if(Array.isArray(res)) rawEvents=res;
  else if(Array.isArray(res.events)) rawEvents=res.events;
  else{
    log('Poll "'+sig.name+'" — unexpected shape: '+JSON.stringify(res).slice(0,150),'red');
    return {real:0,completions:0,errors:1,reason:'bad_shape'};
  }

  log('Poll "'+sig.name.slice(0,22)+'" — '+rawEvents.length+' raw event(s) returned','gray');

  let real=0,completions=0,errors=0,dupes=0;

  for(const ev of rawEvents){
    const evType=(ev.type||'').toLowerCase();

    if(evType==='completion'||(ev.monitor_ts&&!ev.output&&evType!=='event')){
      completions++;
      continue;
    }

    if(evType==='error'){
      const errMsg=ev.message||ev.error||ev.output||'unknown';
      log('Monitor run error "'+sig.name+'" (Parallel internal): '+errMsg,'amber');
      completions++;
      continue;
    }

    const rawEventId = ev.event_id||ev.id||null;
    const rawGroupId = ev.event_group_id||null;
    const contentSlug = (ev.output||ev.description||ev.content||'').slice(0,48).replace(/\W/g,'');
    const eid = rawEventId
      ? String(rawEventId)
      : rawGroupId
        ? String(rawGroupId)
        : 'ev_'+sig.id+'_'+(ev.event_date||'')+'_'+contentSlug.slice(0,32);

    if(DB.events.find(e=>e.event_id===eid)){ dupes++; continue; }

    const outputText=ev.output||ev.description||ev.content||ev.text||ev.summary||'';
    if(!outputText||outputText.startsWith('{"type":"completion"')||outputText.startsWith('[{"type":')){
      completions++;
      continue;
    }

    const structured=ev.structured_output||ev.structured||ev.fields||{};
    const companyName=(structured.company_name||ev.company_name||'').trim();
    const companyDomain=(structured.company_domain||ev.company_domain||'')
      .replace(/^https?:\/\//,'').split('/')[0].trim();

    const companyLinkedinUrl=(structured.company_linkedin_url||ev.company_linkedin_url||'').trim();

    const newEv={
      event_id:eid,
      signal_id:sig.id, signal_name:sig.name, category:sig.category,
      type:'event',
      output:outputText,
      source_urls:ev.source_urls||[],
      event_date:ev.event_date||today(),
      fetched_at:new Date().toISOString(),
      enrichment:null,
      company_name:companyName,
      company_domain:companyDomain,
      company_linkedin_url:companyLinkedinUrl
    };

    DB.events.unshift(newEv);
    real++;

    setTimeout(()=>enrichEvent(eid), real*3000); // 3s stagger to prevent concurrent GPT flood

    log('New event: ✓ ['+sig.name.slice(0,20)+'] company="'+companyName+'" domain="'+companyDomain+'" — '+outputText.slice(0,60),'green');
  }

  const parts=[];
  if(real>0)        parts.push(real+' new signal'+(real>1?'s':''));
  if(dupes>0)       parts.push(dupes+' already-seen skipped');
  if(completions>0) parts.push(completions+' completion run'+(completions>1?'s':'')+' (no signal)');
  log('Poll "'+sig.name.slice(0,22)+'" — '+(parts.length?parts.join(' · '):'nothing returned'),real>0?'green':'gray');

  return {real,completions,errors};
}

async function pollAllNow(){
  if(pollPaused){ log('Poll skipped — polling is paused','gray'); return; }
  if(pollRunning){ log('Poll skipped — previous poll still running','gray'); return; }
  var _now=Date.now();
  if(window._lastPollStarted && (_now-window._lastPollStarted)<60000){
    log('Poll skipped — cooldown ('+(Math.round((_now-window._lastPollStarted)/1000))+'s ago)','gray');
    return;
  }
  if(!getActiveKey()){showAlert('Add an API key first.','error');return;}
  window._lastPollStarted=_now;
  pollRunning = true;
  const active = DB.signals.filter(s=>s.status==='active'&&s.monitor_id);
  const inactive = DB.signals.filter(s=>s.status!=='active'||!s.monitor_id);

  if(!active.length){
    pollRunning = false;
    if(inactive.length) showAlert('No active signals with monitors. Check Signals tab.','warning');
    else showAlert('No signals yet — create one in the Signals tab.','info');
    return;
  }

  var _pk=getActiveKey();
  var _pkVal=_pk?(_pk.key||'').slice(0,8)+'...'+((_pk.key||'').slice(-4)):'none';
  log('── Poll started: '+active.length+' monitor(s) | key: '+_pkVal,'blue');
  const startTime = Date.now();
  document.getElementById('poll-indicator').style.display='flex';

  const results = await Promise.all(active.map(sig => pollSignal(sig).catch(e=>{
    log('Unhandled error polling "'+sig.name+'": '+e.message,'red');
    return {real:0,completions:0,errors:1};
  })));

  const elapsed = ((Date.now()-startTime)/1000).toFixed(1);
  let totalReal=0, totalComp=0, totalErr=0;
  const perSignal = [];

  active.forEach(function(sig,i){
    const r=results[i];
    totalReal += r.real||0;
    totalComp += r.completions||0;
    totalErr  += r.errors||0;
    if(r.real>0)        perSignal.push({icon:'✓', name:sig.name, detail:r.real+' new signal'+(r.real>1?'s':''), color:'green'});
    else if(r.errors>0) perSignal.push({icon:'✗', name:sig.name, detail:'API error — '+( r.reason||'check log'), color:'red'});
    else                perSignal.push({icon:'◦', name:sig.name, detail:'ran, no new signals', color:'gray'});
  });

  DB.meta.lastPoll = new Date().toISOString();
  document.getElementById('last-poll').textContent='polled '+istTime(new Date().toISOString());
  document.getElementById('poll-indicator').style.display='none';
  pollRunning = false; // release lock
  save(); updateMetrics(); renderEvents();

  log('── Poll done in '+elapsed+'s: '+totalReal+' new signal'+(totalReal!==1?'s':'')+' | '+totalComp+' completions | '+totalErr+' API errors ──',
    totalReal>0?'green':totalErr>0?'amber':'blue');
  perSignal.forEach(function(s){
    log(s.icon+' '+s.name.slice(0,28)+': '+s.detail, s.color);
  });

  if(totalReal>0){
    showAlert('🎯 '+totalReal+' new signal event'+(totalReal>1?'s':'')+' detected!','success');
  } else if(totalErr>0){
    showAlert('⚠ '+totalErr+' monitor API error'+(totalErr>1?'s':'')+' (key/network issue) — check Activity log','error');
  } else {
    const lastReal = DB.events.filter(e=>e.type==='event').sort((a,b)=>new Date(b.fetched_at)-new Date(a.fetched_at))[0];
    const sinceMsg = lastReal ? 'Last signal: '+timeSince(lastReal.fetched_at) : 'No signals collected yet';
    showAlert('✓ '+active.length+' monitor'+(active.length>1?'s':'')+' checked — no new signals. '+sinceMsg,'info',5000);
  }
}

function timeSince(isoStr){
  if(!isoStr) return 'unknown';
  const diff = Date.now() - new Date(isoStr).getTime();
  const m = Math.floor(diff/60000);
  const h = Math.floor(m/60);
  const d = Math.floor(h/24);
  const rel = d>0?d+'d ago':h>0?h+'h ago':m>0?m+'m ago':'just now';
  return rel+' ('+istDateTime(isoStr)+')';
}

async function debugDumpEvents(){
  const all=DB.events;
  log('══ DEBUG: '+all.length+' total stored events ══','blue');
  const byType={};
  all.forEach(function(e){ byType[e.type]=(byType[e.type]||0)+1; });
  Object.keys(byType).forEach(function(t){ log('  type="'+t+'": '+byType[t]+' event(s)','gray'); });

  log('── ALL stored event IDs ('+all.length+' total): ──','blue');
  all.forEach(function(e,i){
    log((i+1)+'. ['+e.type+'] id='+e.event_id+' | '+(e.output||e.monitor_ts||'').slice(0,50),'gray');
  });

  const activeSigs=DB.signals.filter(function(s){return s.status==='active'&&s.monitor_id;});
  if(!activeSigs.length){ log('No active signals to compare','amber'); log('══ END DEBUG ══','blue'); return; }

  log('── Live Parallel fetch (14d lookback) to compare IDs ──','blue');
  for(var si=0;si<activeSigs.length;si++){
    var sig=activeSigs[si];
    log('Fetching: "'+sig.name+'" monitor='+sig.monitor_id,'gray');
    var res=await pFetch('GET','/v1alpha/monitors/'+sig.monitor_id+'/events?lookback=14d');
    if(!res||res.__pFetchError){ log('Fetch failed for "'+sig.name+'"','red'); continue; }
    var rawEvs=Array.isArray(res)?res:(Array.isArray(res.events)?res.events:[]);
    log('Parallel returned '+rawEvs.length+' raw event(s) for "'+sig.name+'"','gray');
    rawEvs.forEach(function(ev,i){
      var evType=(ev.type||'unknown');
      var rawEventId=ev.event_id||ev.id||null;
      var rawGroupId=ev.event_group_id||null;
      var contentSlug2=(ev.output||ev.description||ev.content||'').slice(0,48).replace(/\W/g,'');
      var eid=rawEventId?String(rawEventId):rawGroupId?String(rawGroupId):'ev_'+sig.id+'_'+(ev.event_date||'')+'_'+contentSlug2.slice(0,32);
      var matchedEntry=DB.events.find(function(e){return e.event_id===eid;});
      var inDB=!!matchedEntry;
      var backwardsMatch=!inDB&&rawGroupId?DB.events.find(function(e){return e.event_id===String(rawGroupId);}):null;
      var status=evType==='completion'?'[COMPLETION — skip]'
        :evType==='error'?'[ERROR — skip]'
        :inDB?'[DUPE — exact match: '+matchedEntry.event_id.slice(0,30)+'...]'
        :backwardsMatch?'[DUPE — backwards match]'
        :'[NEW ← would ingest]';
      var color=evType==='completion'?'gray':(inDB||backwardsMatch)?'amber':'green';
      log('  '+(i+1)+'. type='+evType+' fullId='+eid+' '+status,color);
    });
  }
  log('══ END DEBUG ══','blue');
  showAlert('Debug dump written to Activity log','info',3000);
}

async function pollOneSignal(id){
  const sig=DB.signals.find(s=>s.id===id); if(!sig) return;
  if(!sig.monitor_id){ showAlert('No monitor ID — recreate this signal.','error'); return; }
  document.getElementById('poll-indicator').style.display='flex';
  log('Manual poll: "'+sig.name+'"...','gray');
  const r=await pollSignal(sig);
  document.getElementById('poll-indicator').style.display='none';
  DB.meta.lastPoll=new Date().toISOString();
  save(); updateMetrics(); renderEvents();
  if(r.real>0) showAlert('🎯 '+r.real+' new event(s) for "'+sig.name+'"!','success');
  else if(r.errors>0) showAlert('Monitor error for "'+sig.name+'" — check Activity log','error');
  else{
    const lastReal=DB.events.filter(e=>e.type==='event'&&e.signal_id===sig.id).sort((a,b)=>new Date(b.fetched_at)-new Date(a.fetched_at))[0];
    const sinceMsg=lastReal?'Last signal '+timeSince(lastReal.fetched_at):'No signals yet';
    showAlert('Monitor checked — no new signals. '+sinceMsg,'info');
  }
}

function startPollTimer(){
  if(pollTimer) clearInterval(pollTimer);
  const mins=parseInt(DB.settings.pollInterval)||30;
  pollTimer=setInterval(()=>{ pollAllNow(); updateNextPoll(); }, mins*60*1000);
  updateNextPoll();
}

function toggleEventGroup(domain){
  _expandedEventGroups[domain] = _expandedEventGroups[domain] === false ? true : false;
  renderEvents();
}

function toggleEventDetail(eventId){
  _expandedEvents[eventId] = !_expandedEvents[eventId];
  renderEvents();
}

function logoImgError(img){
  var fav=img.dataset.fav;
  if(fav&&img.src!==fav){ img.src=fav; return; }
  var fc=img.dataset.fc||'?';
  img.parentElement.outerHTML='<div style="width:44px;height:44px;border-radius:10px;background:var(--accent-dim);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:700;color:var(--accent);flex-shrink:0">'+fc+'</div>';
}

function toggleSidebar(){
  var sb=document.querySelector('.sidebar');
  var btn=document.getElementById('sidebar-toggle-btn');
  var isCollapsed=sb.classList.toggle('collapsed');
  if(btn){ btn.textContent=isCollapsed?'›':'‹'; btn.title=isCollapsed?'Expand sidebar':'Collapse sidebar'; }
  try{ localStorage.setItem('dmand_sidebar_collapsed', isCollapsed?'1':'0'); }catch(e){}
}

var FUNCTION_MIGRATION = {
  'General Management': 'Operations',
  'Customer Success': 'Support',
  'Data Science': 'Research',
  'Healthcare': 'Healthcare Services',
  'Media': 'Media and Communication',
  'Program Management': 'Program and Project Management',
  'Project Management': 'Program and Project Management',
};

function migratePersonaFunctions(){
  if(!DB.personas) return;
  DB.personas.forEach(function(p){
    if(!p.dept) return;
    var parts = p.dept.split(',').map(function(d){ return d.trim(); }).filter(Boolean);
    var migrated = parts.map(function(d){ return FUNCTION_MIGRATION[d] || d; });
    p.dept = migrated.join(',');
  });
}

function initSidebarState(){
  try{
    var collapsed=localStorage.getItem('dmand_sidebar_collapsed');
    if(collapsed==='1'){
      var sb=document.querySelector('.sidebar');
      var btn=document.getElementById('sidebar-toggle-btn');
      if(sb) sb.classList.add('collapsed');
      if(btn){ btn.textContent='›'; btn.title='Expand sidebar'; }
    }
  }catch(e){}
}

function togglePausePolling(){
  pollPaused=!pollPaused;
  var btn=document.getElementById('btn-pause-poll');
  var pollBtn=document.getElementById('btn-poll-now');
  if(pollPaused){
    if(pollTimer){ clearInterval(pollTimer); pollTimer=null; }
    if(btn){ btn.textContent='▶ Resume'; btn.style.background='rgba(22,163,74,0.1)'; btn.style.borderColor='rgba(22,163,74,0.3)'; btn.style.color='var(--green)'; }
    if(pollBtn) pollBtn.disabled=true;
    var nextEl=document.getElementById('next-poll-label');
    if(nextEl) nextEl.textContent='⏸ Polling paused';
    log('⏸ Polling paused — no new events will be fetched until resumed','amber');
    showAlert('Polling paused. Click Resume when you wake up.','warning',5000);
  } else {
    if(pollBtn) pollBtn.disabled=false;
    if(btn){ btn.textContent='⏸ Pause'; btn.style.background='rgba(245,158,11,0.1)'; btn.style.borderColor='rgba(245,158,11,0.3)'; btn.style.color='var(--amber)'; }
    startPollTimer();
    log('▶ Polling resumed','green');
    showAlert('Polling resumed.','success',3000);
  }
}

document.addEventListener('visibilitychange', function(){
  if(document.visibilityState==='visible'){
    const mins=parseInt(DB.settings.pollInterval)||30;
    const lastPoll=DB.meta.lastPoll?new Date(DB.meta.lastPoll):null;
    var _minGap=Math.max(5*60*1000, mins*60*1000);
    const overdue=!lastPoll||(Date.now()-lastPoll.getTime())>_minGap;
    if(!pollPaused && overdue && getActiveKey() && DB.signals.filter(function(s){return s.status==='active';}).length){
      log('Tab visible — overdue poll, running now...','gray');
      pollAllNow();
    }
    updateNextPoll();
  }
});
function updateNextPoll(){
  const mins=parseInt(DB.settings.pollInterval)||30;
  const next=new Date(Date.now()+mins*60*1000);
  const el=document.getElementById('next-poll-label');
  if(el) el.textContent='next poll '+istTime(next.toISOString());
}

function toggleCompletions(){
  showCompletions=!showCompletions;
  document.getElementById('show-completions-toggle').className='toggle'+(showCompletions?' on':'');
  renderEvents();
}

var _editingPersonaId = null;

function editPersona(id){
  var p=DB.personas.find(function(x){return x.id===id;});
  if(!p) return;
  _editingPersonaId=id;

  document.getElementById('icp-name').value=p.name||'';
  document.getElementById('icp-titles').value=p.titles||'';
  document.getElementById('icp-similar-titles').value=p.similar_titles||'true';
  document.getElementById('icp-keyword').value=p.keyword||'';
  document.getElementById('icp-country').value=p.location||'';
  var orgLocEl=document.getElementById('icp-org-location'); if(orgLocEl) orgLocEl.value=p.org_location||'';
  var revMinEl=document.getElementById('icp-revenue-min'); if(revMinEl) revMinEl.value=p.revenue_min||'';
  var revMaxEl=document.getElementById('icp-revenue-max'); if(revMaxEl) revMaxEl.value=p.revenue_max||'';
  var orgJobEl=document.getElementById('icp-org-job-titles'); if(orgJobEl) orgJobEl.value=p.org_job_titles||'';
  var minJobEl=document.getElementById('icp-min-jobs'); if(minJobEl) minJobEl.value=p.min_jobs||'';
  var maxJobEl=document.getElementById('icp-max-jobs'); if(maxJobEl) maxJobEl.value=p.max_jobs||'';
  var techAnyEl=document.getElementById('icp-tech-any'); if(techAnyEl) techAnyEl.value=p.tech_any||'';
  var techAllEl=document.getElementById('icp-tech-all'); if(techAllEl) techAllEl.value=p.tech_all||'';
  var techNoneEl=document.getElementById('icp-tech-none'); if(techNoneEl) techNoneEl.value=p.tech_none||'';
  document.getElementById('icp-notes').value=p.notes||'';
  var sigSel=document.getElementById('icp-signal-assign');
  if(sigSel) sigSel.value=p.signal_id||'';
  var aiPromptEl=document.getElementById('icp-ai-prompt');
  if(aiPromptEl) aiPromptEl.value=p.ai_prompt||'';

  document.querySelectorAll('.icp-seniority-cb').forEach(function(cb){
    cb.checked=(p.seniority||'').split(',').map(function(s){return s.trim();}).indexOf(cb.value)>=0;
  });
  document.querySelectorAll('.icp-function-cb').forEach(function(cb){
    var vals=(p.dept||'').split(',').map(function(s){return s.trim();});
    cb.checked=vals.indexOf(cb.value)>=0;
  });
  document.querySelectorAll('.icp-email-status-cb').forEach(function(cb){
    var vals=(p.email_status||'').split(',').map(function(s){return s.trim();});
    cb.checked=vals.indexOf(cb.value)>=0;
  });
  document.querySelectorAll('.icp-headcount-cb').forEach(function(cb){
    var vals=(p.headcount||'').split('|').map(function(s){return s.trim();});
    cb.checked=vals.indexOf(cb.value)>=0;
  });

  document.getElementById('icp-form-title').textContent='Edit ICP persona — '+p.name;
  document.getElementById('icp-save-btn').textContent='Update persona →';
  document.getElementById('icp-cancel-btn').style.display='';
  document.getElementById('icp-editing-badge').style.display='';

  document.getElementById('icp-form-title').scrollIntoView({behavior:'smooth',block:'start'});
}

function cancelEditPersona(){
  _editingPersonaId=null;
  document.getElementById('icp-name').value='';
  document.getElementById('icp-titles').value='';
  document.getElementById('icp-similar-titles').value='true';
  document.getElementById('icp-keyword').value='';
  document.getElementById('icp-country').value='';
  document.getElementById('icp-notes').value='';
  var fields=['icp-org-location','icp-revenue-min','icp-revenue-max','icp-org-job-titles','icp-min-jobs','icp-max-jobs','icp-tech-any','icp-tech-all','icp-tech-none','icp-ai-prompt'];
  var sigSel=document.getElementById('icp-signal-assign'); if(sigSel) sigSel.value='';
  fields.forEach(function(fid){ var el=document.getElementById(fid); if(el) el.value=''; });
  document.querySelectorAll('.icp-seniority-cb,.icp-function-cb,.icp-email-status-cb,.icp-headcount-cb').forEach(function(cb){cb.checked=false;});
  document.getElementById('icp-form-title').textContent='Create ICP Persona';
  document.getElementById('icp-save-btn').textContent='Save persona →';
  document.getElementById('icp-cancel-btn').style.display='none';
  document.getElementById('icp-editing-badge').style.display='none';
}

function populateIcpSignalDropdown(){
  var sel=document.getElementById('icp-signal-assign');
  if(!sel) return;
  var current=sel.value;
  sel.innerHTML='<option value="">— No signal assigned —</option>';
  (DB.signals||[]).forEach(function(s){
    var opt=document.createElement('option');
    opt.value=s.id;
    opt.textContent=s.name.slice(0,50)+(s.name.length>50?'…':'');
    sel.appendChild(opt);
  });
  if(current) sel.value=current;
}

function onIcpSignalChange(){
  var statusEl=document.getElementById('icp-ai-status');
  if(statusEl) statusEl.textContent='';
}

async function icpFillByAI(){
  var oaiKey=DB.settings&&DB.settings.oaiKey;
  if(!oaiKey){ showAlert('Add your OpenAI API key in the Keys tab first.','error'); return; }

  var sigSel=document.getElementById('icp-signal-assign');
  var sigId=sigSel?sigSel.value:'';
  if(!sigId){ showAlert('Assign a signal first — select one from the dropdown above.','warning'); return; }

  var sig=DB.signals.find(function(s){return s.id===sigId;});
  if(!sig){ showAlert('Signal not found.','error'); return; }

  var btn=document.getElementById('icp-ai-fill-btn');
  var spinner=document.getElementById('icp-ai-fill-spinner');
  var statusEl=document.getElementById('icp-ai-status');
  btn.disabled=true;
  if(spinner) spinner.style.display='inline-block';
  if(statusEl) statusEl.textContent='Analyzing signal + recent events…';

  try{
    var recentEvents=(DB.events||[])
      .filter(function(e){ return e.signal_id===sigId && e.type==='event' && e.output; })
      .slice(0,5)
      .map(function(e){ return '- '+e.output.replace(/\s+/g,' ').trim().slice(0,200); })
      .join('\n');

    var companyContext=[
      DB.outreach&&DB.outreach.companyBrief ? 'Company: '+DB.outreach.companyBrief : '',
      DB.outreach&&DB.outreach.valueProp ? 'Value prop: '+DB.outreach.valueProp : '',
      DB.outreach&&DB.outreach.painPoints ? 'Pain points: '+DB.outreach.painPoints : '',
    ].filter(Boolean).join('\n');

    var manualAiPrompt='';
    var aiPromptEl=document.getElementById('icp-ai-prompt');
    if(aiPromptEl) manualAiPrompt=aiPromptEl.value.trim();

    var promptParts=[
      'You are an expert B2B sales strategist.',
      'TASK: Build a precise ICP persona for outreach to companies that match this specific signal. Different signals indicate different company actions — reason from the signal to identify WHO is the right decision maker.',
      'SIGNAL BEING MONITORED:',
      'Name: '+sig.name,
      'Query: '+sig.query,
      sig.notes?'Notes: '+sig.notes:'',
      recentEvents?('RECENT SIGNAL EVENTS (real company examples):\n'+recentEvents):'',
      companyContext?('SELLER CONTEXT (what you sell):\n'+companyContext):'',
      manualAiPrompt?('ADDITIONAL INSTRUCTIONS:\n'+manualAiPrompt):'',
      'Return ONLY a valid JSON object with these exact fields:',
      '{',
      '  "persona_name": "short descriptive name e.g. VP of Clinical Operations",',
      '  "seniority": ["c_suite","vp"] — array, only use: founder,owner,c_suite,partner,vp,head,director,manager,senior,entry,intern',
      '  "titles": "pipe-separated exact job titles e.g. Chief Medical Officer|VP of Clinical Informatics",',
      '  "dept": ["sales","operations"] — array, only use: accounting,administrative,arts_and_design,business_development,consulting,education,engineering,entrepreneurship,finance,healthcare,human_resources,information_technology,legal,marketing,media_and_communication,operations,product_management,public_relations,real_estate,sales,support',
      '  "keyword": "1-3 domain-specific keywords",',
      '  "location": "person location e.g. United States",',
      '  "org_location": "company HQ e.g. United States",',
      '  "headcount": ["51,200","201,500"] — array, only use: 1,10 | 11,50 | 51,200 | 201,500 | 501,1000 | 1001,5000 | 5001,10000 | 10001,1000000',
      '  "revenue_min": null or number in USD e.g. 1000000,',
      '  "revenue_max": null or number in USD e.g. 50000000,',
      '  "tech_any": "pipe-separated techs e.g. salesforce|hubspot or empty string",',
      '  "org_job_titles": "pipe-separated active hiring roles relevant to this signal or empty string",',
      '  "notes": "1 sentence: what this signal means + why this persona is the right target"',
      '}',
      'RULES:',
      '- A funding signal targets different people than a hiring signal or leadership change signal — be specific.',
      '- Do NOT produce the same generic persona for every signal.',
      '- seniority and dept must use only the exact enum values listed above.',
      '- Return ONLY the JSON, no markdown, no explanation.'
    ].filter(function(l){return l!==null&&l!==undefined&&l!=='';}).join('\n');
    var prompt=promptParts;

    var r=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+oaiKey},
      body:JSON.stringify({
        model:DB.settings.oaiModel||'gpt-4o-mini',
        max_tokens:800,
        temperature:0.3,
        messages:[{role:'user',content:prompt}]
      }),
      signal:AbortSignal.timeout(30000)
    });

    if(!r.ok){
      var errTxt=await r.text();
      throw new Error('GPT HTTP '+r.status+': '+errTxt.slice(0,100));
    }

    var data=await r.json();
    var raw=(data.choices&&data.choices[0]&&data.choices[0].message&&data.choices[0].message.content)||'';
    raw=raw.replace(/^```json\s*/,'').replace(/^```\s*/,'').replace(/\s*```$/,'').trim();

    var parsed;
    try{ parsed=JSON.parse(raw); }
    catch(e){ throw new Error('Could not parse GPT response: '+raw.slice(0,100)); }

    var applied=[];

    if(parsed.persona_name){
      var nameEl=document.getElementById('icp-name');
      if(nameEl){ nameEl.value=parsed.persona_name; applied.push('name'); }
    }

    if(parsed.seniority&&Array.isArray(parsed.seniority)&&parsed.seniority.length){
      document.querySelectorAll('.icp-seniority-cb').forEach(function(cb){ cb.checked=false; });
      var senSet=parsed.seniority.map(function(s){return s.trim().toLowerCase();});
      document.querySelectorAll('.icp-seniority-cb').forEach(function(cb){
        if(senSet.indexOf(cb.value.toLowerCase())>=0){ cb.checked=true; }
      });
      applied.push('seniority');
    }

    if(parsed.dept&&Array.isArray(parsed.dept)&&parsed.dept.length){
      document.querySelectorAll('.icp-function-cb').forEach(function(cb){ cb.checked=false; });
      var deptSet=parsed.dept.map(function(d){return d.trim().toLowerCase();});
      document.querySelectorAll('.icp-function-cb').forEach(function(cb){
        if(deptSet.indexOf(cb.value.toLowerCase())>=0){ cb.checked=true; }
      });
      applied.push('department');
    }

    if(parsed.headcount&&Array.isArray(parsed.headcount)&&parsed.headcount.length){
      document.querySelectorAll('.icp-headcount-cb').forEach(function(cb){ cb.checked=false; });
      var hcSet=parsed.headcount.map(function(h){return h.trim();});
      document.querySelectorAll('.icp-headcount-cb').forEach(function(cb){
        if(hcSet.indexOf(cb.value)>=0){ cb.checked=true; }
      });
      applied.push('headcount');
    }

    var textFields=[
      ['icp-titles','titles'],
      ['icp-keyword','keyword'],
      ['icp-country','location'],
      ['icp-org-location','org_location'],
      ['icp-tech-any','tech_any'],
      ['icp-org-job-titles','org_job_titles'],
      ['icp-notes','notes'],
    ];
    textFields.forEach(function(pair){
      var el=document.getElementById(pair[0]);
      if(el&&parsed[pair[1]]){
        el.value=parsed[pair[1]];
        applied.push(pair[1]);
      }
    });

    if(parsed.revenue_min){
      var rmEl=document.getElementById('icp-revenue-min');
      if(rmEl){ rmEl.value=parsed.revenue_min; applied.push('revenue_min'); }
    }
    if(parsed.revenue_max){
      var rxEl=document.getElementById('icp-revenue-max');
      if(rxEl){ rxEl.value=parsed.revenue_max; applied.push('revenue_max'); }
    }

    if(statusEl) statusEl.textContent='✓ AI filled: '+applied.join(', ')+'. Review and adjust before saving.';
    statusEl.style.color='var(--green)';
    log('ICP Fill by AI: filled '+applied.length+' fields for signal "'+sig.name+'"','green');

  }catch(e){
    if(statusEl){ statusEl.textContent='✗ '+e.message; statusEl.style.color='var(--red)'; }
    log('ICP Fill by AI error: '+e.message,'red');
  }finally{
    btn.disabled=false;
    if(spinner) spinner.style.display='none';
  }
}

function createPersona(){
  const name = document.getElementById('icp-name').value.trim();
  const titles = document.getElementById('icp-titles').value.trim();
  const similarTitles = document.getElementById('icp-similar-titles').value;
  const functionCbs = document.querySelectorAll('.icp-function-cb:checked');
  const dept = Array.from(functionCbs).map(function(cb){return cb.value;}).join(',');
  const keyword = document.getElementById('icp-keyword').value.trim();
  const emailStatusCbs = document.querySelectorAll('.icp-email-status-cb:checked');
  const emailStatus = Array.from(emailStatusCbs).map(function(cb){return cb.value;}).join(',');
  const personLocation = document.getElementById('icp-country').value.trim();
  const orgLocation = document.getElementById('icp-org-location').value.trim();
  const headcountCbs = document.querySelectorAll('.icp-headcount-cb:checked');
  const headcount = Array.from(headcountCbs).map(function(cb){return cb.value;}).join('|');
  const revenueMin = document.getElementById('icp-revenue-min').value.trim();
  const revenueMax = document.getElementById('icp-revenue-max').value.trim();
  const orgJobTitles = document.getElementById('icp-org-job-titles').value.trim();
  const minJobs = document.getElementById('icp-min-jobs').value.trim();
  const maxJobs = document.getElementById('icp-max-jobs').value.trim();
  const techAny = document.getElementById('icp-tech-any').value.trim();
  const techAll = document.getElementById('icp-tech-all').value.trim();
  const techNone = document.getElementById('icp-tech-none').value.trim();
  const notes = document.getElementById('icp-notes').value.trim();
  const seniorityCbs = document.querySelectorAll('.icp-seniority-cb:checked');
  const seniority = seniorityCbs.length
    ? Array.from(seniorityCbs).map(cb=>cb.value).join(',')
    : 'any';
  if(!name){showAlert('Enter a persona name.','error');return;}
  if(!DB.personas) DB.personas=[];

  const assignedSignalId = document.getElementById('icp-signal-assign') ?
    document.getElementById('icp-signal-assign').value : '';
  const aiPrompt = (document.getElementById('icp-ai-prompt') ?
    document.getElementById('icp-ai-prompt').value.trim() : '');

  const personaData = {
    name, titles,
    similar_titles: similarTitles,
    seniority, dept,
    keyword,
    email_status: emailStatus,
    location: personLocation,
    org_location: orgLocation,
    headcount,
    revenue_min: revenueMin,
    revenue_max: revenueMax,
    org_job_titles: orgJobTitles,
    min_jobs: minJobs,
    max_jobs: maxJobs,
    tech_any: techAny,
    tech_all: techAll,
    tech_none: techNone,
    notes,
    signal_id: assignedSignalId || null,
    ai_prompt: aiPrompt || null
  };

  if(_editingPersonaId){
    var p=DB.personas.find(function(x){return x.id===_editingPersonaId;});
    if(p){ Object.assign(p, personaData); p.updated_at=new Date().toISOString(); save(); renderPersonas(); showAlert('Persona "'+name+'" updated.','success'); }
    cancelEditPersona();
    return;
  }

  DB.personas.push(Object.assign({id:'per_'+Date.now(), created_at:new Date().toISOString()}, personaData));
  cancelEditPersona();
  save(); renderPersonas();
  showAlert('Persona "'+name+'" saved.','success');
}

function deletePersona(id){
  const p=DB.personas.find(x=>x.id===id);
  if(!p||!confirm(`Delete persona "${p.name}"?`)) return;
  DB.personas=DB.personas.filter(x=>x.id!==id);
  saveAndSync(); renderPersonas();
}

function renderPersonas(){
  if(!DB.personas) DB.personas=[];
  document.getElementById('nb-personas').textContent=DB.personas.length;
  const el=document.getElementById('persona-list');
  if(!el) return;
  if(!DB.personas.length){
    el.innerHTML=`<div class="empty"><div class="empty-icon">◈</div><div class="empty-title">No personas yet</div><div class="empty-sub">Create an ICP persona above to use with Find People</div></div>`;
    return;
  }
  el.innerHTML=DB.personas.map(p=>`
    <div class="persona-card">
      <div class="persona-card-top">
        <div style="flex:1">
          <div class="persona-name">${esc(p.name)}</div>
          ${p.signal_id ? (() => { var sig=DB.signals.find(function(s){return s.id===p.signal_id;}); return sig ? '<div style="font-size:10px;font-family:var(--mono);color:var(--accent);margin-bottom:4px">⚡ '+esc(sig.name)+'</div>' : ''; })() : ''}
          <div class="persona-meta" style="margin-top:6px;display:flex;flex-wrap:wrap;gap:4px">
            ${p.seniority&&p.seniority!=='any'?'<span class="pill pill-amber" title="Seniority">⬆ '+esc(p.seniority.split(',').map(function(s){return s.trim();}).join(', '))+'</span>':'<span class="pill pill-gray">Any seniority</span>'}
            ${p.titles?'<span class="pill pill-purple" title="Titles">◆ '+esc(p.titles.replace(/\|/g,', '))+'</span>':''}
            ${p.dept?p.dept.split(',').map(function(d){return d.trim();}).filter(Boolean).map(function(d){return '<span class="pill pill-blue" title="Department">▨ '+esc(d)+'</span>';}).join(''):''}
            ${p.keyword?'<span class="pill pill-gray" title="Keyword">🔍 '+esc(p.keyword)+'</span>':''}
            ${p.location?'<span class="pill pill-gray" title="Person location">📍 '+esc(p.location)+'</span>':''}
            ${p.org_location?'<span class="pill pill-gray" title="Company HQ">🏢 HQ: '+esc(p.org_location)+'</span>':''}
            ${p.headcount?p.headcount.split('|').map(function(h){return '<span class="pill pill-green" title="Headcount">👥 '+esc(h)+'</span>';}).join(''):''}
            ${p.revenue_min||p.revenue_max?'<span class="pill pill-green" title="Revenue">💰 $'+(p.revenue_min?Math.round(p.revenue_min/1000)+'K':'0')+' – '+(p.revenue_max?'$'+Math.round(p.revenue_max/1000)+'K':'∞')+'</span>':''}
            ${p.tech_any?'<span class="pill pill-blue" title="Uses any">⚙ '+esc(p.tech_any.replace(/\|/g,', '))+'</span>':''}
            ${p.tech_none?'<span class="pill pill-red" title="Excludes tech">✗ '+esc(p.tech_none.replace(/\|/g,', '))+'</span>':''}
            ${p.org_job_titles?'<span class="pill pill-amber" title="Hiring for">📋 Hiring: '+esc(p.org_job_titles.replace(/\|/g,', '))+'</span>':''}
            ${p.email_status?'<span class="pill pill-green" title="Email status">✉ '+esc(p.email_status)+'</span>':''}
            ${p.notes?'<span class="pill pill-gray" style="max-width:240px;overflow:hidden;text-overflow:ellipsis;font-style:italic">'+esc(p.notes)+'</span>':''}
          </div>
        </div>
        <div style="display:flex;gap:6px;flex-shrink:0">
          <button class="btn btn-sm" onclick="editPersona('${p.id}')">Edit</button>
          <button class="btn btn-sm btn-danger" onclick="deletePersona('${p.id}')">Delete</button>
        </div>
      </div>
    </div>`).join('');
}

function clearContacts(){ if(!confirm('Clear all contacts?')) return; DB.contacts=[]; saveAndSync(); renderContacts(); document.getElementById('nb-contacts').textContent='0'; }

function updateFindPeopleBar(eventId){  }

let contactPage = 0;
const CONTACTS_PER_PAGE = 50;
let eventPage = 0;
const EVENTS_PER_PAGE = 25;
let companyPage = 0;
const COMPANIES_PER_PAGE = 10;
let campaignPage = 0;
const CAMPAIGNS_PER_PAGE = 5;
let selectedContactIds = new Set();

function toggleContactSelection(id, checked){
  if(checked) selectedContactIds.add(id);
  else selectedContactIds.delete(id);
  updateSelectionUI();
}

function toggleSelectAll(checked){
  const cbs = document.querySelectorAll('.contact-row-cb');
  cbs.forEach(function(cb){
    cb.checked = checked;
    if(checked) selectedContactIds.add(cb.dataset.id);
    else selectedContactIds.delete(cb.dataset.id);
  });
  updateSelectionUI();
}

function updateSelectionUI(){
  const count = selectedContactIds.size;
  const delBtn = document.getElementById('delete-selected-btn');
  const delCount = document.getElementById('selected-count');
  const enrichBtn = document.getElementById('enrich-emails-btn');
  const enrichCount = document.getElementById('enrich-count');
  const sendBtn = document.getElementById('send-selected-btn');
  const sendCount = document.getElementById('send-count');
  if(delBtn) delBtn.style.display = count > 0 ? '' : 'none';
  if(delCount) delCount.textContent = count;
  const feKey = DB.settings.fullenrichKey;
  const withLi = count > 0 && feKey
    ? [...selectedContactIds].filter(id=>{ const c=DB.contacts.find(x=>x.id===id); return c&&c.linkedin; }).length
    : 0;
  if(enrichBtn) enrichBtn.style.display = withLi > 0 ? '' : 'none';
  if(enrichCount) enrichCount.textContent = withLi;
  const slKey = DB.outreach&&DB.outreach.smartleadKey;
  const withEmail = count > 0 && slKey
    ? [...selectedContactIds].filter(id=>{ const c=DB.contacts.find(x=>x.id===id); return c&&(c.business_email&&c.business_email!=='N/A'); }).length
    : 0;
  if(sendBtn) sendBtn.style.display = withEmail > 0 ? '' : 'none';
  if(sendCount) sendCount.textContent = withEmail;
  const allCbs = document.querySelectorAll('.contact-row-cb');
  const selectAllCb = document.getElementById('contact-select-all');
  if(selectAllCb && allCbs.length){
    const checkedCount = [...allCbs].filter(cb=>cb.checked).length;
    selectAllCb.indeterminate = checkedCount > 0 && checkedCount < allCbs.length;
    selectAllCb.checked = checkedCount === allCbs.length;
  }
}

function deleteSelectedContacts(){
  if(!selectedContactIds.size) return;
  if(!confirm('Delete ' + selectedContactIds.size + ' selected contact(s)?')) return;
  DB.contacts = DB.contacts.filter(function(c){ return !selectedContactIds.has(c.id); });
  selectedContactIds.clear();
  saveAndSync();
  document.getElementById('delete-selected-btn').style.display = 'none';
  document.getElementById('selected-count').textContent = '0';
  renderContacts();
  document.getElementById('nb-contacts').textContent = DB.contacts.filter(c=>c.name&&c.linkedin).length;
}

function renderContacts(resetPage){
  if(typeof currentPage !== 'undefined' && currentPage !== 'contacts') return;
  if(resetPage !== false) contactPage = 0;
  if(!DB.contacts) DB.contacts=[];
  if(!DB.companies) DB.companies=[];
  if(!DB.autoboundKeys) DB.autoboundKeys=[];

  const el=document.getElementById('contact-list');
  if(!el) return;

  const sf=document.getElementById('contact-sig-filter');
  const curSig=sf.value;
  sf.innerHTML='<option value="">All signals</option>'+DB.signals.map(s=>`<option value="${s.id}" ${curSig===s.id?'selected':''}>${esc(s.name)}</option>`).join('');

  const pf=document.getElementById('contact-persona-filter');
  const curPer=pf.value;
  if(!DB.personas) DB.personas=[];
  pf.innerHTML='<option value="">All personas</option>'+DB.personas.map(p=>`<option value="${p.id}" ${curPer===p.id?'selected':''}>${esc(p.name)}</option>`).join('');

  const df=document.getElementById('contact-dept-filter');
  const curDept=df?df.value:'';
  const depts=[...new Set(DB.contacts.map(c=>c.department).filter(Boolean))].sort();
  if(df){
    df.innerHTML='<option value="">All departments</option>'+depts.map(d=>`<option ${curDept===d?'selected':''}>${esc(d)}</option>`).join('');
    if(curDept) df.value=curDept; // restore selection after innerHTML reset
  }

  const search=(document.getElementById('contact-search')?.value||'').toLowerCase();
  const sigFilter=sf.value;
  const perFilter=pf.value;
  const deptFilter=df?df.value:'';
  const senFilter=document.getElementById('contact-seniority-filter')?.value||'';

  let cons=[...DB.contacts].filter(c=>
    c.name && c.linkedin
  ).sort((a,b)=>new Date(b.found_at)-new Date(a.found_at));

  if(search) cons=cons.filter(c=>(c.name+c.title+c.company+c.domain+(c.department||'')).toLowerCase().includes(search));
  if(sigFilter){ const sig=DB.signals.find(s=>s.id===sigFilter); if(sig) cons=cons.filter(c=>c.signal_name===sig.name||c.event_id&&DB.events.find(e=>e.event_id===c.event_id&&e.signal_id===sigFilter)); }
  if(perFilter) cons=cons.filter(c=>c.persona_id===perFilter);
  if(deptFilter) cons=cons.filter(c=>(c.department||'').toLowerCase()===deptFilter.toLowerCase());
  if(senFilter) cons=cons.filter(c=>{
    const s=(c.seniority||'').toLowerCase();
    const f=senFilter.toLowerCase();
    return s===f || s.includes(f) || f.includes(s);
  });
  const bizEmailFilter = (document.getElementById('contact-biz-email-filter')||{value:''}).value;
  if(bizEmailFilter==='yes') cons=cons.filter(c=>c.business_email && c.business_email!=='N/A' && c.business_email!=='');
  if(bizEmailFilter==='no')  cons=cons.filter(c=>c.email_status==='done' && (!c.business_email || c.business_email==='N/A'));
  var conDays=parseInt(document.getElementById('contact-date-filter')?.value||'0')||0;
  if(conDays>0){
    var conCutoff=new Date(Date.now()-conDays*864e5).toISOString();
    cons=cons.filter(function(c){ return (c.found_at||'')>=conCutoff; });
  }

  document.getElementById('nb-contacts').textContent=cons.length;

  if(!cons.length){
    el.innerHTML=`<div class="empty"><div class="empty-icon">◯</div><div class="empty-title">No contacts yet</div><div class="empty-sub">Click "Find People" on any Company card to search using Crustdata Person Search.</div></div>`;
    return;
  }

  const totalPages=Math.ceil(cons.length/CONTACTS_PER_PAGE);
  if(contactPage>=totalPages) contactPage=totalPages-1;
  const pageStart=contactPage*CONTACTS_PER_PAGE;
  const pageCons=cons.slice(pageStart,pageStart+CONTACTS_PER_PAGE);

  const paginationHtml = totalPages>1 ? `
    <div style="display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-top:1px solid var(--border);background:var(--surface);font-size:12px;font-family:var(--mono)">
      <span style="color:var(--text3)">Showing ${pageStart+1}–${Math.min(pageStart+CONTACTS_PER_PAGE,cons.length)} of ${cons.length}</span>
      <div style="display:flex;gap:6px;align-items:center">
        <button class="btn btn-sm" onclick="contactPage=Math.max(0,contactPage-1);renderContacts(false)" ${contactPage===0?'disabled':''}>← Prev</button>
        <span style="color:var(--text2);padding:0 8px">Page ${contactPage+1} / ${totalPages}</span>
        <button class="btn btn-sm" onclick="contactPage=Math.min(totalPages-1,contactPage+1);renderContacts(false)" ${contactPage>=totalPages-1?'disabled':''}>Next →</button>
      </div>
    </div>` : '';

  el.innerHTML=`<div class="contacts-table-wrap"><table class="contacts-table">
    <thead><tr>
      <th style="width:32px;padding:8px;text-align:center"><input type="checkbox" id="contact-select-all" onchange="toggleSelectAll(this.checked)" style="cursor:pointer;accent-color:var(--accent)"></th>
      <th style="width:160px">Contact</th>
      <th style="width:150px">Title & Headline</th>
      <th style="width:110px">Company</th>
      <th style="width:100px">Signal</th>
      <th style="width:90px">ICP</th>
      <th style="width:120px">Campaign</th>
      <th style="width:75px">Seniority</th>
      <th style="width:80px">Dept</th>
      <th style="width:110px">Location</th>
      <th style="width:175px">Email</th>
      <th style="width:70px">Insights</th>
      <th style="width:90px">Send</th>
      <th style="width:90px">LinkedIn</th>
      <th style="width:65px">Reply</th>
    </tr></thead>
    <tbody>${pageCons.map(c=>{
      const colors=['#d97757','#7c3aed','#2563eb','#16a34a','#d97706'];
      const aColor=colors[(c.name||'?').charCodeAt(0)%colors.length];
      const initial=(c.name||'?')[0].toUpperCase();
      const avatar=c.photo_url
        ? '<img src="'+esc(c.photo_url)+'" style="width:30px;height:30px;border-radius:50%;object-fit:cover;border:1px solid var(--border);flex-shrink:0" onerror="this.style.display=\'none\'">'
        : '<div style="width:30px;height:30px;border-radius:50%;background:'+aColor+';flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#fff">'+initial+'</div>';
      const emailBtn=c.email_status==='enriching'
        ? '<span style="font-size:10px;color:var(--text3)">enriching…</span>'
        : (c.business_email&&c.business_email!=='N/A')
          ? '<a href="mailto:'+esc(c.business_email)+'" style="color:var(--accent);font-size:11px;font-family:var(--mono)">'+esc(c.business_email)+'</a>'
          : c.email_status==='done'
            ? '<span style="font-size:10px;color:var(--text3)">N/A</span>'
            : c.linkedin
              ? '<button onclick="enrichSingleEmail(\''+c.id+'\')" class="c-action-btn email">✉ Get email</button>'
              : '<span style="font-size:10px;color:var(--text3)">no LinkedIn</span>';
      const insightsCnt=(c.autobound_insights||[]).length;
      const insightsBtn=c.insights_status==='loading'
        ? '<span style="font-size:10px;color:var(--text3)">…</span>'
        : c.insights_status==='done'
          ? '<button onclick="openInsightsPanel(\''+c.id+'\')" class="c-action-btn insights">💡 '+insightsCnt+'</button>'
          : '<button onclick="getInsightsSingle(\''+c.id+'\')" class="c-action-btn insights">💡 get</button>';
      const camps=(DB.campaigns||[]).filter(function(cp){return cp.mode;});

      var sigForC=DB.signals.find(function(s){return s.id===c.signal_id;});
      var tgtForC=sigForC&&sigForC.icp_targets&&sigForC.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;});
      var hasSigCamp=!!(tgtForC&&tgtForC.campaign_id);
      var sendEl='';
      if(c.sl_reply_status==='replied'){
        sendEl='<span class="pill pill-green" style="font-size:10px">Replied</span>';
      } else if(c.sl_reply_status==='bounced'){
        sendEl='<span class="pill pill-red" style="font-size:10px">Bounced</span>';
      } else if(c.smartlead_campaign_id){
        sendEl='<span style="font-size:10px;color:var(--green);font-weight:600">Sent ✓</span>'+(c.smartlead_pushed_at?'<div style="font-size:9px;color:var(--text3);font-family:var(--mono)">'+istDateTime(c.smartlead_pushed_at)+'</div>':'');
      } else if(c.business_email&&c.business_email!=='N/A'){
        var _slTarget=null;
        var _slSig=c.signal_id?DB.signals.find(function(s){return s.id===c.signal_id;}):null;
        if(_slSig&&_slSig.icp_targets){
          _slTarget=_slSig.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;})||_slSig.icp_targets[0];
        }
        if(_slTarget&&_slTarget.campaign_id){
          sendEl='<button onclick="oneClickPushSmartlead(\''+c.id+'\',this)" class="c-action-btn" style="white-space:nowrap">📤 Smartlead</button>';
        } else {
          sendEl='<span style="font-size:10px;color:var(--text3)" title="No Smartlead campaign assigned to this signal ICP">📤 —</span>';
        }
      } else {
        sendEl='<span style="font-size:10px;color:var(--text3)">no email</span>';
      }

      var liBtn='';
      if(c.heyreach_campaign_id){
        var liStatus=c.linkedin_status||'pending';
        liBtn=(liStatus==='replied'?'<span class="pill pill-green" style="font-size:10px">Replied</span>':liStatus==='connected'?'<span style="font-size:10px;color:var(--green);font-weight:600">Connected</span>':'<span style="font-size:10px;color:#0a66c2;font-weight:600">Sent ✓</span>')+(c.heyreach_pushed_at?'<div style="font-size:9px;color:var(--text3);font-family:var(--mono)">'+istDateTime(c.heyreach_pushed_at)+'</div>':'');
      } else if(c.linkedin){
        var _hrTarget=null;
        var _hrSig=c.signal_id?DB.signals.find(function(s){return s.id===c.signal_id;}):null;
        if(_hrSig&&_hrSig.icp_targets){
          _hrTarget=_hrSig.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;})||_hrSig.icp_targets[0];
        }
        if(_hrTarget&&_hrTarget.hr_campaign_id){
          liBtn='<button onclick="oneClickPushHeyreach(\''+c.id+'\',this)" class="c-action-btn" style="border-color:rgba(10,102,194,0.28);color:#0a66c2;background:rgba(10,102,194,0.04);white-space:nowrap">💼 HeyReach</button>';
        } else {
          liBtn='<span style="font-size:10px;color:var(--text3)" title="No HeyReach campaign assigned to this signal ICP">💼 —</span>';
        }
      } else {
        liBtn='<span style="font-size:10px;color:var(--text3)">—</span>';
      }
      const replyBadge=c.sl_reply_status==='replied'
        ? '<span class="pill pill-green" style="font-size:10px">Replied</span>'
        : c.sl_reply_status==='bounced'
          ? '<span class="pill pill-red" style="font-size:10px">Bounced</span>'
          : c.sl_reply_status==='completed'
            ? '<span style="font-size:10px;color:var(--text3)">Done</span>'
            : c.smartlead_campaign_id
              ? '<span style="font-size:10px;color:var(--green);font-weight:600">Sent ✓</span>'+(c.smartlead_pushed_at?'<div style="font-size:9px;color:var(--text3);font-family:var(--mono)">'+istDateTime(c.smartlead_pushed_at)+'</div>':'')
              : '<span style="color:var(--text3)">—</span>';
      var sen=c.seniority||'';
      if(!sen&&c.title){
        var tl=(c.title+' '+(c.headline||'')).toLowerCase();
        if(/\bfounder\b|\bco-founder\b|\bceo\b|\bcoo\b|\bcto\b|\bcfo\b|\bcmo\b|\bcro\b|\bchief\b|\bpresident\b/.test(tl)) sen='C-Suite';
        else if(/\bsvp\b|\bevp\b|\bvp\b|\bvice president\b/.test(tl)) sen='VP';
        else if(/\bdirector\b|\bhead of\b/.test(tl)) sen='Director';
        else if(/\bmanager\b/.test(tl)) sen='Manager';
        else if(/\bsenior\b|\bsr\.\b|\blead\b|\bprincipal\b/.test(tl)) sen='Senior IC';
      }
      return '<tr class="'+(selectedContactIds.has(c.id)?'selected-row':'')+'">'
        +'<td style="text-align:center;padding:10px 8px"><input type="checkbox" class="contact-row-cb" data-id="'+c.id+'" '+(selectedContactIds.has(c.id)?'checked':'')+' onchange="toggleContactSelection(\''+c.id+'\',this.checked)" style="cursor:pointer;accent-color:var(--accent)"></td>'
        +'<td style="white-space:nowrap"><div style="display:flex;align-items:center;gap:8px">'+avatar
          +'<div style="min-width:0"><div style="font-weight:600;font-size:12px;color:var(--text)">'+esc(c.name||'—')+'</div>'
          +(c.linkedin?'<a href="'+esc(c.linkedin)+'" target="_blank" style="font-size:10px;color:var(--blue);font-family:var(--mono);text-decoration:none">↗ LinkedIn</a>':'')
          +'<div style="margin-top:2px">'
          +(c.source_type==='automated'
            ?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(22,163,74,0.1);color:var(--green);border:1px solid rgba(22,163,74,0.2);font-family:var(--mono);font-weight:600">⚡ Auto</span>'
            :'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:var(--surface3);color:var(--text3);border:1px solid var(--border);font-family:var(--mono)">Manual</span>')
          +'</div>'
          +(c.found_at?'<div style="font-size:9px;color:var(--text3);font-family:var(--mono);margin-top:2px">'+istDateTime(c.found_at)+'</div>':'')
          +'</div></div></td>'
        +'<td><div style="font-size:12px;font-weight:500;color:var(--text);overflow:hidden;text-overflow:ellipsis;max-width:140px;white-space:nowrap" title="'+esc(c.title||'')+'">'+esc(c.title||'—')+'</div>'
          +(c.headline?'<div style="font-size:10px;color:var(--text3);overflow:hidden;text-overflow:ellipsis;max-width:140px;white-space:nowrap">'+esc(c.headline.slice(0,50))+'</div>':'')
          +'</td>'
        +'<td><div style="font-size:12px;font-weight:500;color:var(--text);overflow:hidden;text-overflow:ellipsis;max-width:100px;white-space:nowrap">'+esc(c.company||'—')+'</div>'
          +(c.domain?'<div style="font-size:10px;color:var(--text3);font-family:var(--mono)">'+esc(c.domain)+'</div>':'')
          +'</td>'
        +'<td><div style="font-size:10px;color:var(--accent);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;max-width:90px;white-space:nowrap" title="'+esc(c.signal_name||'')+'">'+esc(c.signal_name||'—')+'</div>'
          +(c.event_date?'<div style="font-size:10px;color:var(--text3)">'+istDate(c.event_date)+'</div>':'')
          +'</td>'
        +'<td>'+(c.icp_persona_name||c.persona_name
          ? '<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:600;background:rgba(124,58,237,0.08);color:var(--purple);border:1px solid rgba(124,58,237,0.18);white-space:nowrap;max-width:85px;overflow:hidden;text-overflow:ellipsis" title="'+esc(c.icp_persona_name||c.persona_name||'')+'">⚡ '+esc((c.icp_persona_name||c.persona_name||'').slice(0,12))+'</span>'
          : '<span style="color:var(--text3);font-size:10px">—</span>')+'</td>'
        +'<td>'+(c.smartlead_campaign_name
          ? '<span style="display:inline-flex;align-items:center;gap:3px;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:600;background:rgba(37,99,235,0.08);color:var(--blue);border:1px solid rgba(37,99,235,0.18);white-space:nowrap;max-width:110px;overflow:hidden;text-overflow:ellipsis" title="'+esc(c.smartlead_campaign_name||'')+'">📤 '+esc((c.smartlead_campaign_name||'').slice(0,14))+'</span>'
          : '<span style="color:var(--text3);font-size:10px">—</span>')+'</td>'
        +'<td>'+(sen?'<span class="pill pill-purple" style="font-size:10px">'+esc(sen)+'</span>':'<span style="color:var(--text3)">—</span>')+'</td>'
        +'<td style="font-size:11px;color:var(--text2)">'+esc(c.department||'—')+'</td>'
        +'<td style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;max-width:100px;white-space:nowrap">'+esc(c.location||'—')+'</td>'
        +'<td>'+emailBtn+'</td>'
        +'<td>'+insightsBtn+'</td>'
        +'<td>'+sendEl+'</td>'
        +'<td>'+liBtn+'</td>'
        +'<td>'+replyBadge+'</td>'
        +'</tr>';
    }).join('')}
    </tbody>
  </table>${paginationHtml}</div>`;
}

function renderTemplates(){
  document.getElementById('template-grid').innerHTML=TEMPLATES.map((t,i)=>`
    <div class="tpl-card" onclick="loadTemplateItem(${i})">
      <div style="font-size:12px;font-weight:700;margin-bottom:4px">${t.name}</div>
      <div style="display:flex;gap:5px;margin-bottom:6px"><span class="pill pill-gray">${t.category}</span><span class="pill pill-gray">${t.frequency}</span></div>
      <div style="font-size:11px;color:var(--text3);line-height:1.4;font-family:var(--mono)">${t.query.slice(0,90)}...</div>
    </div>`).join('');
}
function toggleTemplates(){ const p=document.getElementById('template-panel'); p.style.display=p.style.display==='none'?'':'none'; }
function loadTemplateItem(i){
  const t=TEMPLATES[i];
  document.getElementById('sig-name').value=t.name;
  document.getElementById('sig-query').value=t.query;
  document.getElementById('sig-freq').value=t.frequency;
  const sel=document.getElementById('sig-cat'); for(let o of sel.options) if(o.text===t.category){sel.value=o.value;break;}
  document.getElementById('template-panel').style.display='none';
  showAlert('Template loaded. Review and click Create signal.','info');
}

function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function toggleSignalEdit(id){
  const panel=document.getElementById('edit-panel-'+id);
  if(!panel) return;
  panel.classList.toggle('open');
}

function renderEditICPTargets(sig){
  var html='';
  var targets=sig.icp_targets||[];
  var slCamps=(DB.campaigns||[]).filter(function(c){return c.mode;});
  var hrCamps=(DB.hrCampaigns||[]).filter(function(c){return c.hr_mode&&c.hr_mode!=='';});
  if(!targets.length){
    return '<div style="font-size:11px;color:var(--text3);padding:6px 0">No ICP targets — click + Add target to set up auto-enrich</div>';
  }
  targets.forEach(function(t,ti){
    var sid=sig.id;
    var slOpts='<option value="">📧 No email campaign</option>';
    slCamps.forEach(function(c){
      slOpts+='<option value="'+c.id+'"'+(String(t.campaign_id)===String(c.id)?' selected':'')+'>'+esc(c.name.slice(0,20))+'</option>';
    });
    var hrOpts='<option value="">💼 No LinkedIn campaign</option>';
    hrCamps.filter(function(c){return c.hr_mode;}).forEach(function(c){
      var modeLabel={'signal':'📡','signal_ib':'🧊'}[c.hr_mode]||'';
      hrOpts+='<option value="'+c.hr_id+'"'+(String(t.hr_campaign_id)===String(c.hr_id)?' selected':'')+'>'+modeLabel+' '+esc(c.hr_name.slice(0,22))+'</option>';
    });
    hrCamps.filter(function(c){return !c.hr_mode;}).forEach(function(c){
      hrOpts+='<option value="'+c.hr_id+'"'+(String(t.hr_campaign_id)===String(c.hr_id)?' selected':'')+'>'+esc(c.hr_name.slice(0,25))+'</option>';
    });
    html+='<div class="edit-icp-row" id="edit-icp-row-'+sid+'-'+ti+'" style="display:grid;grid-template-columns:1fr 60px 1fr 1fr auto;gap:6px;align-items:center;margin-bottom:6px;padding:8px 10px;background:var(--surface2);border-radius:7px;border:1px solid var(--border)">'
      +'<span style="font-size:11px;font-weight:600;color:var(--accent);font-family:var(--mono)">⚡ '+esc(t.persona_name||'—')+'</span>'
      +'<input type="number" min="1" max="50" value="'+(t.max_contacts||5)+'" id="edit-icp-max-'+sid+'-'+ti+'" class="form-input" style="font-size:11px;padding:4px 6px;text-align:center" title="Max contacts">'
      +'<select class="form-select" id="edit-icp-sl-'+sid+'-'+ti+'" style="font-size:11px;padding:4px 6px" title="📧 Email campaign">'+slOpts+'</select>'
      +'<select class="form-select" id="edit-icp-hr-'+sid+'-'+ti+'" style="font-size:11px;padding:4px 6px;border-color:rgba(10,102,194,0.3);color:#0a66c2" title="💼 LinkedIn campaign">'+hrOpts+'</select>'
      +'<button onclick="removeEditICPTarget(this)" data-sig="'+sid+'" data-ti="'+ti+'" style="background:none;border:none;color:var(--red);cursor:pointer;font-size:14px;padding:2px 6px" title="Remove">✕</button>'
      +'</div>';
  });
  return html;
}

function addEditICPTarget(sigId){
  var sig=DB.signals.find(function(s){return s.id===sigId;});
  if(!sig) return;
  if(!sig.icp_targets) sig.icp_targets=[];
  var personas=(DB.personas||[]);
  if(!personas.length){ showAlert('Create ICP personas first in the ICP tab.','warning'); return; }
  var picker='<div style="display:grid;grid-template-columns:1fr 60px 1fr 1fr auto;gap:6px;align-items:center;margin-bottom:6px;padding:8px 10px;background:var(--surface2);border-radius:7px;border:1px solid var(--accent)">'
    +'<select id="new-icp-persona-'+sigId+'" class="form-select" style="font-size:11px;padding:4px 6px">'
    +'<option value="">— Pick persona —</option>'
    +personas.map(function(p){return '<option value="'+p.id+'">'+esc(p.name)+'</option>';}).join('')
    +'</select>'
    +'<input type="number" min="1" max="50" value="5" id="new-icp-max-'+sigId+'" class="form-input" style="font-size:11px;padding:4px 6px;text-align:center" placeholder="Max">'
    +'<select id="new-icp-sl-'+sigId+'" class="form-select" style="font-size:11px;padding:4px 6px">'
    +'<option value="">📧 No email campaign</option>'
    +(DB.campaigns||[]).filter(function(c){return c.mode;}).map(function(c){return '<option value="'+c.id+'">'+esc(c.name.slice(0,20))+'</option>';}).join('')
    +'</select>'
    +'<select id="new-icp-hr-'+sigId+'" class="form-select" style="font-size:11px;padding:4px 6px;border-color:rgba(10,102,194,0.3);color:#0a66c2">'
    +'<option value="">💼 No LinkedIn</option>'
    +(DB.hrCampaigns||[]).filter(function(c){return c.hr_mode&&c.hr_mode!=='';}).map(function(c){var ml={'signal':'📡','signal_ib':'🧊'}[c.hr_mode]||'';return '<option value="'+c.hr_id+'">'+ml+' '+esc(c.hr_name.slice(0,22))+'</option>';}).join('')
    +'</select>'
    +'<button onclick="confirmAddEditICPTarget(this)" data-sig="'+sigId+'" style="background:var(--accent);border:none;color:#fff;cursor:pointer;font-size:11px;padding:4px 10px;border-radius:5px;white-space:nowrap">Add ✓</button>'
    +'</div>';
  var list=document.getElementById('edit-icp-list-'+sigId);
  if(list){
    var existing=document.getElementById('new-icp-row-'+sigId);
    if(existing) existing.remove();
    var div=document.createElement('div');
    div.id='new-icp-row-'+sigId;
    div.innerHTML=picker;
    list.appendChild(div);
  }
}

function confirmAddEditICPTarget(btn){
  var sigId=btn.getAttribute('data-sig');
  var sig=DB.signals.find(function(s){return s.id===sigId;});
  if(!sig) return;
  var personaSel=document.getElementById('new-icp-persona-'+sigId);
  var maxInp=document.getElementById('new-icp-max-'+sigId);
  var slSel=document.getElementById('new-icp-sl-'+sigId);
  var hrSel=document.getElementById('new-icp-hr-'+sigId);
  if(!personaSel||!personaSel.value){ showAlert('Select a persona.','warning'); return; }
  var persona=(DB.personas||[]).find(function(p){return p.id===personaSel.value;});
  if(!persona) return;
  var slId=slSel?slSel.value:'';
  var slCamp=slId?(DB.campaigns||[]).find(function(c){return String(c.id)===String(slId);}):null;
  var hrId=hrSel?hrSel.value:'';
  var hrCamp=hrId?(DB.hrCampaigns||[]).find(function(c){return String(c.hr_id)===String(hrId);}):null;
  if(!sig.icp_targets) sig.icp_targets=[];
  sig.icp_targets.push({
    persona_id:persona.id,
    persona_name:persona.name,
    max_contacts:parseInt(maxInp?maxInp.value:5)||5,
    campaign_id:slId||null,
    campaign_name:slCamp?slCamp.name:null,
    hr_campaign_id:hrId||null,
    hr_campaign_name:hrCamp?hrCamp.hr_name:null,
    hr_mode:hrCamp?hrCamp.hr_mode:''
  });
  save();
  renderSignals();
  showAlert('ICP target added. Click Save changes to apply.','success');
}

function removeEditICPTarget(btn){
  var sigId=btn.getAttribute('data-sig');
  var idx=parseInt(btn.getAttribute('data-ti'));
  var sig=DB.signals.find(function(s){return s.id===sigId;});
  if(!sig||!sig.icp_targets) return;
  sig.icp_targets.splice(idx,1);
  save();
  renderSignals();
  setTimeout(function(){ toggleSignalEdit(sigId); toggleSignalEdit(sigId); }, 50);
}

async function saveSignalEdit(id){
  const sig=DB.signals.find(s=>s.id===id);
  if(!sig) return;
  const statusEl=document.getElementById('edit-status-'+id);
  const newName=(document.getElementById('edit-name-'+id).value||'').trim();
  const newQuery=(document.getElementById('edit-query-'+id).value||'').trim();
  const newFreq=document.getElementById('edit-freq-'+id).value;
  const newCat=document.getElementById('edit-cat-'+id).value;
  const newNotes=(document.getElementById('edit-notes-'+id).value||'').trim();

  if(!newName){ showAlert('Signal name cannot be empty.','error'); return; }
  if(!newQuery){ showAlert('Query cannot be empty.','error'); return; }

  const queryChanged = newQuery !== sig.query;
  const freqChanged  = newFreq  !== sig.frequency;
  const needsRecreate = (queryChanged || freqChanged) && sig.monitor_id;

  sig.name     = newName;
  sig.query    = newQuery;
  sig.frequency= newFreq;
  sig.category = newCat;
  sig.notes    = newNotes;

  if(sig.icp_targets&&sig.icp_targets.length){
    sig.icp_targets.forEach(function(t,ti){
      var maxEl=document.getElementById('edit-icp-max-'+id+'-'+ti);
      var slEl=document.getElementById('edit-icp-sl-'+id+'-'+ti);
      var hrEl=document.getElementById('edit-icp-hr-'+id+'-'+ti);
      if(maxEl) t.max_contacts=parseInt(maxEl.value)||t.max_contacts;
      if(slEl&&slEl.value!==undefined){
        var slId=slEl.value;
        var slCamp=slId?(DB.campaigns||[]).find(function(c){return String(c.id)===String(slId);}):null;
        t.campaign_id=slId||null;
        t.campaign_name=slCamp?slCamp.name:null;
      }
      if(hrEl&&hrEl.value!==undefined){
        var hrId=hrEl.value;
        var hrCamp=hrId?(DB.hrCampaigns||[]).find(function(c){return String(c.hr_id)===String(hrId);}):null;
        t.hr_campaign_id=hrId||null;
        t.hr_campaign_name=hrCamp?hrCamp.hr_name:null;
        t.hr_mode=hrCamp?hrCamp.hr_mode:'';
      }
    });
  }

  if(needsRecreate){
    if(statusEl) statusEl.textContent = 'Updating monitor on Parallel…';
    await deleteMonitorOnParallel(sig.monitor_id);
    sig.monitor_id = null;
    sig.status = 'pending';
    renderSignals();
    const mid = await createMonitorOnParallel(sig);
    if(mid){
      sig.status = 'active';
      save(); renderSignals(); updateMetrics();
      showAlert('Signal updated — running 14d catch-up poll for new query…','success', 8000);
      const savedLookback = DB.settings.lookback;
      DB.settings.lookback = '14d';
      await new Promise(r=>setTimeout(r, 1500)); // let Parallel index the new monitor
      await pollSignal(sig);
      DB.settings.lookback = savedLookback;
      save(); renderEvents(); updateMetrics();
      showAlert('Catch-up complete — new signals (if any) are now in Events.','info', 6000);
    } else {
      sig.status = 'error';
      save(); renderSignals();
      showAlert('Monitor recreation failed — signal saved locally. Use Retry to fix.','error', 8000);
    }
  } else {
    save(); renderSignals(); updateMetrics();
    showAlert('Signal "'+newName+'" saved.','success', 3000);
  }
}

function renderSignals(){
  const el=document.getElementById('signal-list');
  document.getElementById('nb-signals').textContent=DB.signals.length;
  if(!DB.signals.length){
    el.innerHTML=`<div class="empty"><div class="empty-icon">◎</div><div class="empty-title">No signals yet</div><div class="empty-sub">Create a signal or load a template above</div></div>`;
    return;
  }
  el.innerHTML=DB.signals.map(s=>{
    const realEvs=DB.events.filter(e=>e.signal_id===s.id&&e.type==='event').length;
    const sc=s.status==='active'?'pill-green':s.status==='error'?'pill-red':'pill-amber';
    return `<div class="signal-item">
      <div class="signal-top">
        <div style="flex:1;min-width:0">
          <div class="signal-name">${esc(s.name)}</div>
          <div class="signal-query">${esc(s.query.slice(0,200))}${s.query.length>200?'…':''}</div>
          <div class="signal-meta">
            <span class="pill ${sc}">${s.status}</span>
            <span class="pill pill-gray">${esc(s.category)}</span>
            <span class="pill pill-gray">${s.frequency}</span>
            ${s.key_label?`<span class="pill pill-blue">key: ${esc(s.key_label)}</span>`:''}
            <span class="pill ${realEvs>0?'pill-green':'pill-gray'}">${realEvs} signal event${realEvs!==1?'s':''}</span>
            ${s.created_at?`<span class="pill pill-gray">${istDateTime(s.created_at)}</span>`:''}
          </div>
          ${(s.icp_targets&&s.icp_targets.length)?`<div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:8px;align-items:center">
            <span style="font-size:9px;font-family:var(--mono);color:var(--text3);text-transform:uppercase;letter-spacing:0.05em">Auto-enrich:</span>
            ${s.icp_targets.map(t=>`<span style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:600;background:rgba(22,163,74,0.08);color:var(--green);border:1px solid rgba(22,163,74,0.2)">⚡ ${esc(t.persona_name)} <span style="opacity:0.7">×${t.max_contacts}</span>${t.campaign_name?'<span style="opacity:0.6;font-size:9px">📧 '+esc(t.campaign_name)+'</span>':''} ${t.hr_campaign_name?'<span style="opacity:0.6;font-size:9px;color:#0a66c2">💼 '+esc(t.hr_campaign_name)+'</span>':''}</span>`).join('')}
          </div>`:''}
          ${s.monitor_id?`<div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-top:5px">monitor_id: ${esc(s.monitor_id)}</div>`:''}
        </div>
        <div class="signal-actions">
          <button class="btn btn-sm" onclick="toggleSignalEdit('${s.id}')">✏ Edit</button>
          <button class="btn btn-sm" onclick="pollOneSignal('${s.id}')">Poll</button>
          ${s.status==='error'?`<button class="btn btn-sm btn-amber" onclick="retrySignal('${s.id}')">Retry</button>`:''}
          <button class="btn btn-sm btn-danger" onclick="deleteSignal('${s.id}')">Delete</button>
        </div>
      </div>
      <div class="signal-edit-panel" id="edit-panel-${s.id}">
        <div class="signal-edit-grid">
          <div>
            <label class="form-label">Signal name</label>
            <input class="form-input" id="edit-name-${s.id}" value="${esc(s.name)}" style="font-size:13px">
          </div>
          <div>
            <label class="form-label">Frequency</label>
            <select class="form-select" id="edit-freq-${s.id}" style="font-size:13px">
              <option value="1h" ${s.frequency==='1h'?'selected':''}>Hourly</option>
              <option value="1d" ${s.frequency==='1d'?'selected':''}>Daily</option>
              <option value="3d" ${s.frequency==='3d'?'selected':''}>Every 3 days</option>
              <option value="1w" ${s.frequency==='1w'?'selected':''}>Weekly</option>
            </select>
          </div>
          <div>
            <label class="form-label">Category</label>
            <select class="form-select" id="edit-cat-${s.id}" style="font-size:13px">
              <option ${s.category==='Expansion signal'?'selected':''}>Expansion signal</option>
              <option ${s.category==='Churn risk signal'?'selected':''}>Churn risk signal</option>
              <option ${s.category==='Competitive signal'?'selected':''}>Competitive signal</option>
              <option ${s.category==='Regulatory signal'?'selected':''}>Regulatory signal</option>
              <option ${s.category==='New adopter signal'?'selected':''}>New adopter signal</option>
            </select>
          </div>
        </div>
        <div style="margin-bottom:10px">
          <label class="form-label">Monitor query</label>
          <textarea class="form-textarea" id="edit-query-${s.id}" rows="3" style="font-size:12px;line-height:1.55">${esc(s.query)}</textarea>
          <div class="form-hint">⚠ Changing the query will delete and recreate the monitor on Parallel.</div>
        </div>
        <div style="margin-bottom:4px">
          <label class="form-label">Notes (optional)</label>
          <input class="form-input" id="edit-notes-${s.id}" value="${esc(s.notes||'')}">
        </div>
        <!-- ICP Targets editor — rendered via function to avoid nested template literals -->
        <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:12px">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <label class="form-label" style="margin:0">ICP Targets & Campaigns</label>
            <button class="btn btn-sm btn-ghost" onclick="addEditICPTarget('${s.id}')" style="font-size:11px;padding:3px 10px">+ Add target</button>
          </div>
          <div id="edit-icp-list-${s.id}">${renderEditICPTargets(s)}</div>
        </div>
        <div class="signal-edit-actions">
          <button class="btn btn-accent btn-sm" onclick="saveSignalEdit('${s.id}')">Save changes →</button>
          <button class="btn btn-ghost btn-sm" onclick="toggleSignalEdit('${s.id}')">Cancel</button>
          <span id="edit-status-${s.id}" style="font-size:11px;font-family:var(--mono);color:var(--text3)"></span>
        </div>
      </div>
    </div>`;
  }).join('');
}

function renderEvents(){
  if(typeof currentPage !== 'undefined' && currentPage !== 'events' && currentPage !== 'home') return;
  const el=document.getElementById('event-list');
  const realCount=DB.events.filter(e=>e.type==='event').length;
  document.getElementById('nb-events').textContent=realCount;
  const sf=document.getElementById('event-sig-filter');
  const cur=sf.value;
  sf.innerHTML='<option value="">All signals</option>'+DB.signals.map(s=>`<option value="${s.id}" ${cur===s.id?'selected':''}>${esc(s.name)}</option>`).join('');
  const search=(document.getElementById('event-search')?.value||'').toLowerCase();
  const sigId=sf.value;
  let evs=DB.events;
  evs=evs.filter(e=>{
    if(e.type==='completion'||e.type==='error') return true;
    if(e.type!=='event') return false;
    const o=String(e.output||'').trim();
    if(o.startsWith('{"type":"completion"')||o.startsWith('[{"type":"completion"')) return false;
    if(o.startsWith('{"type":"error"')) return false;
    return true;
  });
  if(!showCompletions) evs=evs.filter(e=>e.type!=='completion');
  if(sigId) evs=evs.filter(e=>e.signal_id===sigId);
  if(search) evs=evs.filter(e=>((e.output||'')+(e.signal_name||'')).toLowerCase().includes(search));
  var evDays=parseInt(document.getElementById('event-date-filter')?.value||'0')||0;
  if(evDays>0){
    var evCutoff=new Date(Date.now()-evDays*864e5).toISOString();
    evs=evs.filter(function(e){ return (e.fetched_at||e.event_date||'')>=evCutoff; });
  }

  if(!evs.length){
    const hasCompletions=DB.events.some(e=>e.type==='completion');
    el.innerHTML=`<div class="empty"><div class="empty-icon">◌</div><div class="empty-title">${realCount===0?'No signal events yet':'No events match your filters'}</div><div class="empty-sub">${realCount===0&&hasCompletions?'Your monitor has run and completed — no new buying signals yet.<br>Daily monitors surface events within ~24h.':'Events with detected content will appear here as cards.'}</div></div>`;
    return;
  }

  var evTotalPages=Math.ceil(evs.length/EVENTS_PER_PAGE);
  if(eventPage>=evTotalPages) eventPage=Math.max(0,evTotalPages-1);
  var evStart=eventPage*EVENTS_PER_PAGE;
  var evSlice=evs.slice(evStart,evStart+EVENTS_PER_PAGE);
  var evPagHtml=evTotalPages>1
    ?'<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 0;margin-top:8px">'
      +'<span style="font-size:11px;font-family:var(--mono);color:var(--text3)">Showing '+(evStart+1)+'–'+Math.min(evStart+EVENTS_PER_PAGE,evs.length)+' of '+evs.length+' events</span>'
      +'<div style="display:flex;align-items:center;gap:6px">'
        +'<button class="btn btn-sm" onclick="eventPage=Math.max(0,eventPage-1);renderEvents()" '+(eventPage===0?'disabled':'')+'>← Prev</button>'
        +'<span style="font-size:11px;font-family:var(--mono);color:var(--text2)">Page '+(eventPage+1)+' / '+evTotalPages+'</span>'
        +'<button class="btn btn-sm" onclick="eventPage=Math.min('+(evTotalPages-1)+',eventPage+1);renderEvents()" '+(eventPage>=evTotalPages-1?'disabled':'')+'>Next →</button>'
      +'</div></div>'
    :'';

  try{
  var realEvs = evSlice.filter(function(e){ return e.type==='event'; });
  var completionEvs = evSlice.filter(function(e){ return e.type==='completion'; });

  var domainGroups = {};
  var noDomainEvents = [];
  realEvs.forEach(function(e){
    var dom = (e.company_domain||'').trim()
      || ((e.enrichment&&e.enrichment.data&&e.enrichment.data.domain)||'').trim()
      || ((e.superEnrichment&&e.superEnrichment.data&&e.superEnrichment.data.domain)||'').trim();
    if(!dom){
      noDomainEvents.push(e);
    } else {
      if(!domainGroups[dom]) domainGroups[dom]={domain:dom, name:'', events:[]};
      if(e.company_name && !domainGroups[dom].name) domainGroups[dom].name=e.company_name;
      if(!domainGroups[dom].name) domainGroups[dom].name=dom;
      domainGroups[dom].events.push(e);
    }
  });
  noDomainEvents.forEach(function(e){
    if(!domainGroups['__unknown__']) domainGroups['__unknown__']={domain:'',name:'Unknown Company',events:[]};
    domainGroups['__unknown__'].events.push(e);
  });

  var html='';

  completionEvs.forEach(function(e){
    html+='<div class="event-completion">'
      +'<span class="event-completion-label">&#9702; Monitor run &mdash; no new signals</span>'
      +'<div style="display:flex;align-items:center;gap:8px">'
        +'<span class="pill pill-gray">'+esc(e.signal_name||'')+'</span>'
        +'<span class="event-completion-date">'+istDate(e.event_date)+'</span>'
      +'</div>'
    +'</div>';
  });

  var domKeys = Object.keys(domainGroups);
  domKeys.forEach(function(dom){
    var grp = domainGroups[dom];
    var evList = grp.events;
    var qualYes = evList.filter(function(e){return e.qualified==='yes';}).length;
    var enrichDone = evList.filter(function(e){return e.superEnrichment&&e.superEnrichment.status==='done';}).length;
    var isUnknown = dom==='__unknown__';
    var faviconUrl = (!isUnknown&&dom) ? 'https://www.google.com/s2/favicons?domain='+encodeURIComponent(dom)+'&sz=32' : '';
    var groupId = 'evgrp_'+(dom||'unknown').replace(/[^a-z0-9]/gi,'_');
    var isExpanded = _expandedEventGroups[dom] !== false; // default expanded

    var qualBadge = qualYes>0
      ? '<span class="pill pill-amber" style="font-size:10px">&#10003; '+qualYes+' ICP</span>'
      : '';
    var enrichBadge = enrichDone>0
      ? '<span class="pill pill-blue" style="font-size:10px">&#9733; Enriched</span>'
      : '';
    var pendingBadge = evList.some(function(e){return !e.enrichment||e.enrichment.status==='loading';})
      ? '<span class="pill pill-gray" style="font-size:10px"><div class="spinner" style="width:8px;height:8px;display:inline-block"></div> Processing</span>'
      : '';

    var initials = esc((grp.name||dom||'?')[0].toUpperCase());
    var logoHtml = faviconUrl
      ? '<img src="'+esc(faviconUrl)+'" width="20" height="20" style="border-radius:4px;object-fit:contain;flex-shrink:0" onerror="this.style.display=String.fromCharCode(110,111,110,101)">'
      : '<div style="width:20px;height:20px;border-radius:4px;background:var(--accent-dim);display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;color:var(--accent);flex-shrink:0">'+initials+'</div>';

    html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);margin-bottom:8px;overflow:hidden;transition:var(--transition)">';
    html += '<div data-dom="'+esc(dom)+'" onclick="toggleEventGroup(this.dataset.dom)"'
      +' style="display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer;user-select:none">'
      + logoHtml
      +'<div style="flex:1;min-width:0">'
        +'<div style="font-size:13px;font-weight:600;color:var(--text);letter-spacing:-0.02em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+esc(grp.name||dom)+'</div>'
        +'<div style="font-size:11px;font-family:var(--mono);color:var(--text3);margin-top:1px">'+(isUnknown?'&mdash;':esc(dom))+'&nbsp;&middot;&nbsp;'+evList.length+' event'+(evList.length!==1?'s':'')+'</div>'
      +'</div>'
      +'<div style="display:flex;align-items:center;gap:5px;flex-shrink:0">'
        +qualBadge+enrichBadge+pendingBadge
        +'<span style="font-size:11px;color:var(--text3);margin-left:2px">'+(isExpanded?'&#9650;':'&#9660;')+'</span>'
      +'</div>'
    +'</div>';

    html += '<div id="'+groupId+'" style="display:'+(isExpanded?'block':'none')+'">';

    evList.forEach(function(e){
      var hasEnrich = e.enrichment&&e.enrichment.status==='done';
      var hasSuperDone = e.superEnrichment&&e.superEnrichment.status==='done';
      var hasSources = e.source_urls&&e.source_urls.length>0;
      var isEvExpanded = _expandedEvents[e.event_id]===true;
      var preview = esc((e.output||'No content yet').replace(/\s+/g,' ').slice(0,110));
      var eid = esc(e.event_id);

      html += '<div style="border-top:1px solid var(--border)">';
      html += '<div data-eid="'+eid+'" onclick="toggleEventDetail(this.dataset.eid)"'
        +' style="display:flex;align-items:center;gap:10px;padding:9px 14px;cursor:pointer;user-select:none">'
        +'<input type="checkbox" class="event-row-cb" data-eid="'+eid+'" onclick="event.stopPropagation()" onchange="updateEventSelection()" style="cursor:pointer;accent-color:var(--accent);flex-shrink:0">'
        +'<span style="font-size:10px;font-family:var(--mono);font-weight:600;color:var(--accent);flex-shrink:0;white-space:nowrap">'+esc(e.signal_name||'')+'</span>'
        +'<div style="flex:1;font-size:12px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+preview+'</div>'
        +'<div style="display:flex;align-items:center;gap:5px;flex-shrink:0">'
          +(e.qualified==='yes'?'<span class="pill pill-amber" style="font-size:9px;padding:1px 5px">&#10003; ICP</span>'
            :e.qualified==='no'?'<span class="pill pill-gray" style="font-size:9px;padding:1px 5px;opacity:0.5">&#10007;</span>'
            :e.enrichment&&e.enrichment.status==='loading'?'<div class="spinner" style="width:9px;height:9px"></div>'
            :'')
          +'<span style="font-size:10px;font-family:var(--mono);color:var(--text3);white-space:nowrap">'+istDate(e.event_date)+'</span>'
          +'<span style="font-size:10px;color:var(--text3)">'+(isEvExpanded?'&#9650;':'&#9660;')+'</span>'
        +'</div>'
      +'</div>';

      html += '<div id="evdet_'+eid+'" style="display:'+(isEvExpanded?'block':'none')+';border-top:1px solid var(--border)">';
      html += '<div class="event-card-body">'
        +'<div class="event-card-output">'
          +(e.output?esc(e.output):'<span style="color:var(--text3);font-style:italic">Processing&hellip;</span>')
        +'</div>'
      +'</div>';

      if(hasSources){
        html+='<div class="event-card-sources">';
        e.source_urls.slice(0,4).forEach(function(u){
          html+='<a class="src-link" href="'+esc(u)+'" target="_blank" title="'+esc(u)+'">'+esc(u.replace(/^https?:\/\//,'').slice(0,55))+'</a>';
        });
        html+='</div>';
      }

      html+='<div class="enrich-action-row" style="padding:9px 14px;border-top:1px solid var(--border);background:var(--surface2);display:flex;align-items:center;gap:8px">';
      if(hasEnrich){
        html+='<button class="enrich-btn-super '+(hasSuperDone?'done':'')+'" id="super-enrich-btn-'+eid+'" data-eid="'+eid+'" onclick="superEnrichEvent(this.dataset.eid)">'
          +'<svg width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.8"><polygon points="6,1 7.5,4.5 11,5 8.5,7.5 9,11 6,9.5 3,11 3.5,7.5 1,5 4.5,4.5"/></svg>'
          +(hasSuperDone?'Re-Super Enrich &#10022;':'Super Enrich &#10022;')
        +'</button>';
        if(hasSuperDone) html+='<span class="enrich-badge" style="background:var(--blue-dim);color:var(--blue);border-color:rgba(29,78,216,0.2)"><svg width="9" height="9" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="1,6 4,9 11,3"/></svg> Crustdata</span>';
      } else if(e.enrichment&&e.enrichment.status==='loading'){
        html+='<span style="font-size:11px;color:var(--text3);font-family:var(--mono);display:flex;align-items:center;gap:6px"><div class="spinner" style="width:10px;height:10px"></div>Pipeline running&hellip;</span>';
      } else if(e.enrichment&&e.enrichment.status==='error'){
        html+='<span style="font-size:11px;color:var(--red);font-family:var(--mono)">&#9888; '+esc((e.enrichment.message||'Error').slice(0,60))+'</span>';
      } else {
        html+='<span style="font-size:11px;color:var(--text3);font-family:var(--mono)">Awaiting enrichment&hellip;</span>';
      }
      html+='</div>';

      html+='<div id="enrich-panel-'+eid+'" style="padding:0 14px 14px;display:'+(hasEnrich?'block':'none')+'"></div>';
      html+='</div>'; // end expanded detail
      html+='</div>'; // end event row
    });

    html+='</div>'; // end group events container
    html+='</div>'; // end group card
  });

  el.innerHTML = html + evPagHtml;
  realEvs.forEach(function(e){
    if(e.enrichment&&e.enrichment.status&&_expandedEvents[e.event_id]) renderEnrichPanel(e.event_id);
  });

  DB.events.filter(function(e){return e.enrichment&&e.enrichment.status;}).forEach(function(e){renderEnrichPanel(e.event_id);});
  }catch(renderErr){ log('renderEvents error: '+renderErr.message+' | stack: '+renderErr.stack.slice(0,300),'red'); el.innerHTML='<div style="color:var(--red);padding:20px;font-family:var(--mono);font-size:12px">Render error: '+esc(renderErr.message)+'</div>'; }
}

function renderKeys(){
  var el=document.getElementById('key-list');
  if(!el) return;
  document.getElementById('nb-keys').textContent=DB.keys.length?1:0;
  if(!DB.keys.length){
    el.innerHTML='<div style="font-size:12px;color:var(--text3);padding:8px 0">No key saved yet.</div>';
    return;
  }
  var k=DB.keys[0];
  var isExhausted=!!k.exhausted;
  var statusBadge=isExhausted
    ?'<span class="pill pill-red" style="font-size:10px">⚠ exhausted — replace key</span>'
    :'<span class="pill pill-green" style="font-size:10px">● active</span>';
  var borderColor=isExhausted?'var(--red)':'var(--accent)';
  var dotClass=isExhausted?'dot-red':'dot-green';
  el.innerHTML='<div style="display:flex;align-items:center;gap:12px;padding:10px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:7px;border-left:3px solid '+borderColor+';'+(isExhausted?'background:rgba(239,68,68,0.03)':'')+'">'
    +'<div class="dot '+dotClass+'" style="flex-shrink:0"></div>'
    +'<div style="flex:1;min-width:0">'
      +'<div style="font-size:12px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:8px">'
        +esc(k.label||'Parallel AI Key')
        +statusBadge
      +'</div>'
      +'<div style="font-size:11px;font-family:var(--mono);color:var(--text3);margin-top:2px">'+k.value.slice(0,12)+'...'+k.value.slice(-6)+'</div>'
    +'</div>'
    +'<div style="display:flex;gap:6px;flex-shrink:0">'
      +'<button class="btn btn-sm" onclick="checkKeyConnection()">Check connection</button>'
    +'</div>'
  +'</div>'
  +(isExhausted?'<div style="margin-top:8px;padding:8px 10px;background:rgba(239,68,68,0.06);border:1px solid rgba(239,68,68,0.2);border-radius:6px;font-size:11px;color:var(--red)">Credits exhausted. Paste a new key below and click Save key →</div>':'');
}

function renderHome(){
  var el=document.getElementById('home-content');
  if(!el) return;

  var now=new Date();
  var today=now.toISOString().split('T')[0];
  var weekAgo=new Date(now-7*864e5).toISOString().split('T')[0];

  var totalSignals=DB.signals.length;
  var activeSignals=DB.signals.filter(function(s){return s.status==='active';}).length;

  var allRealEvents=DB.events.filter(function(e){return e.type==='event';});
  var totalEvents=allRealEvents.length;
  var weekEvents=allRealEvents.filter(function(e){return (e.event_date||'')>=weekAgo;}).length;

  var qualified=allRealEvents.filter(function(e){return e.qualified==='yes';}).length;
  var notQualified=allRealEvents.filter(function(e){return e.qualified==='no';}).length;

  var allCompanies=(DB.companies||[]).filter(function(c){return c.domain;});
  var totalCompanies=allCompanies.length;
  var superEnriched=allCompanies.filter(function(c){return c.enriched_at&&(c.headcount||c.industry||c.hq);}).length;

  var allContacts=(DB.contacts||[]).filter(function(c){return c.name&&c.linkedin;});
  var totalContacts=allContacts.length;
  var autoContacts=allContacts.filter(function(c){return c.source_type==='automated';}).length;
  var manualContacts=allContacts.filter(function(c){return c.source_type!=='automated';}).length;

  var withEmail=allContacts.filter(function(c){return c.business_email&&c.business_email!=='N/A'&&c.business_email.trim()!=='';}).length;

  var sentContacts=allContacts.filter(function(c){return c.smartlead_campaign_id;}).length;
  var autoSent=allContacts.filter(function(c){return c.smartlead_campaign_id&&c.source_type==='automated';}).length;
  var manualSent=allContacts.filter(function(c){return c.smartlead_campaign_id&&c.source_type!=='automated';}).length;

  var replies=allContacts.filter(function(c){return c.sl_reply_status==='replied';}).length;

  var liPushed=allContacts.filter(function(c){return c.heyreach_campaign_id;}).length;
  var liReplied=allContacts.filter(function(c){return c.linkedin_status==='replied';}).length;
  var totalHrCampaigns=(DB.hrCampaigns||[]).length;

  var totalCampaigns=(DB.campaigns||[]).length;

  var recentEvs=allRealEvents.slice().sort(function(a,b){return new Date(b.fetched_at||0)-new Date(a.fetched_at||0);}).slice(0,5);

  var hasCrustKey=!!(DB.settings&&DB.settings.crustdataKey);
  var hasOAIKey=!!(DB.settings&&DB.settings.oaiKey);
  var hasQualifier=!!(DB.settings&&DB.settings.qualifierPrompt);
  var hasSmartlead=!!(DB.outreach&&DB.outreach.smartleadKey)||!!(DB.settings&&DB.settings.smartleadKey);

  function stat(label,val,sub,color,icon,onclick){
    return '<div onclick="'+onclick+'" style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;cursor:'+(onclick?'pointer':'default')+';transition:all 0.15s;box-shadow:var(--shadow-sm)" onmouseenter="this.style.borderColor=\''+color+'\';this.style.transform=\'translateY(-1px)\'" onmouseleave="this.style.borderColor=\'var(--border)\';this.style.transform=\'\'">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">'
        +'<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.07em;color:var(--text3);font-weight:600">'+label+'</div>'
        +'<div style="width:28px;height:28px;border-radius:7px;background:'+color.replace(')',',0.12)').replace('rgb','rgba')+';display:flex;align-items:center;justify-content:center;font-size:14px">'+icon+'</div>'
      +'</div>'
      +'<div style="font-size:28px;font-weight:800;color:var(--text);letter-spacing:-0.02em;line-height:1">'+val+'</div>'
      +(sub?'<div style="font-size:11px;color:var(--text3);margin-top:5px">'+sub+'</div>':'')
      +'</div>';
  }

  function pipeStep(num,label,done,count,color){
    return '<div style="display:flex;align-items:center;gap:10px;padding:10px 14px;border-radius:8px;background:'+(done?'rgba(22,163,74,0.05)':'var(--surface2)')+';border:1px solid '+(done?'rgba(22,163,74,0.2)':'var(--border)')+'">'
      +'<div style="width:22px;height:22px;border-radius:50%;background:'+(done?'var(--green)':'var(--border2)')+';display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:'+(done?'#fff':'var(--text3)')+';flex-shrink:0">'+(done?'✓':num)+'</div>'
      +'<div style="flex:1"><div style="font-size:12px;font-weight:500;color:'+(done?'var(--text)':'var(--text3)')+'">'+label+'</div></div>'
      +(count!==null?'<div style="font-size:13px;font-weight:700;color:'+(done?'var(--green)':'var(--text3)')+'">'+count+'</div>':'')
      +'</div>';
  }

  var autoEvents=qualified; // ICP qualified events
  var autoCompanies=superEnriched;
  var autoContactsAll=autoContacts; // automated contacts with name+linkedin
  var autoEnriched=allContacts.filter(function(c){return c.source_type==='automated'&&c.business_email&&c.business_email!=='N/A'&&c.business_email.trim()!=='';}).length;
  var autoPushed=autoSent; // automated contacts pushed to Smartlead
  var autoReplied=allContacts.filter(function(c){return c.source_type==='automated'&&c.sl_reply_status==='replied';}).length;

  function pct(a,b){ return b>0?Math.round(a/b*100)+'%':'—'; }

  function funnelNode(label, count, sub, color, icon){
    return '<div style="display:flex;flex-direction:column;align-items:center;flex:1;min-width:0">'
      +'<div style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 10px;text-align:center;transition:all 0.15s" onmouseenter="this.style.borderColor=\''+color+'\'" onmouseleave="this.style.borderColor=\'var(--border)\'">'
        +'<div style="font-size:16px;margin-bottom:4px">'+icon+'</div>'
        +'<div style="font-size:22px;font-weight:800;color:var(--text);letter-spacing:-0.02em;line-height:1">'+count+'</div>'
        +'<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);margin-top:4px;font-weight:600">'+label+'</div>'
        +(sub?'<div style="font-size:10px;color:'+color+';margin-top:3px;font-weight:600">'+sub+'</div>':'')
      +'</div>'
    +'</div>';
  }

  function funnelArrow(pctVal){
    return '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0;width:44px;gap:2px">'
      +'<div style="font-size:10px;font-family:var(--mono);color:var(--text3);font-weight:600">'+pctVal+'</div>'
      +'<div style="color:var(--text3);font-size:16px">→</div>'
    +'</div>';
  }

  var html=''
  +'<div style="margin-bottom:24px">'
    +'<div style="font-size:22px;font-weight:800;letter-spacing:-0.02em;color:var(--text)">'+greeting()+' 👋</div>'
    +'<div style="font-size:13px;color:var(--text3);margin-top:4px">'+today+' · '+activeSignals+' active signal'+(activeSignals!==1?'s':'')+' · last poll '+(DB.meta.lastPoll?istDateTime(DB.meta.lastPoll):'never')+'</div>'
  +'</div>'

  +'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">'
    +stat('Signals',totalSignals,activeSignals+' active','#d97757','📡',"showPage('signals',document.querySelector('[onclick*=signals]'))")
    +stat('Events',totalEvents,weekEvents+' this week','#7c3aed','⚡',"showPage('events',document.querySelector('[onclick*=events]'))")
    +stat('Companies',totalCompanies,superEnriched+' enriched','#16a34a','🏢',"showPage('companies',document.querySelector('[onclick*=companies]'))")
    +stat('Contacts',totalContacts,autoContacts+' auto · '+manualContacts+' manual','#2563eb','👤',"showPage('contacts',document.querySelector('[onclick*=contacts]'))")
  +'</div>'

  +'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px">'
    +stat('ICP Qualified',qualified,notQualified?' rej. '+notQualified:'—','#d97706','✓',"showPage('events',document.querySelector('[onclick*=events]'))")
    +stat('With Email',withEmail,totalContacts?Math.round(withEmail/totalContacts*100)+'% of contacts':'—','#7c3aed','✉',"showPage('contacts',document.querySelector('[onclick*=contacts]'))")
    +stat('Emails Sent',sentContacts,autoSent+' auto · '+manualSent+' manual','#d97757','📤',"showPage('contacts',document.querySelector('[onclick*=contacts]'))")
    +stat('LinkedIn Pushed',liPushed,liReplied+' replied','#0a66c2','💼',"showPage('contacts',document.querySelector('[onclick*=contacts]'))")
    +stat('Replies',replies,sentContacts&&replies?Math.round(replies/sentContacts*100)+'% reply rate':'—','#16a34a','💬',"showPage('contacts',document.querySelector('[onclick*=contacts]'))")
  +'</div>'

  +'<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:20px">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">'
      +'<div>'
        +'<div style="font-size:13px;font-weight:700;color:var(--text)">Automated pipeline</div>'
        +'<div style="font-size:11px;color:var(--text3);margin-top:2px;font-family:var(--mono)">ICP qualified events → auto-pushed to Smartlead campaigns</div>'
      +'</div>'
      +'<button onclick="showPage(\'contacts\',document.querySelector(\'[onclick*=contacts]\'))" style="font-size:11px;font-family:var(--mono);color:var(--accent);background:none;border:none;cursor:pointer">View contacts →</button>'
    +'</div>'
    +'<div style="display:flex;align-items:center;gap:0">'
      +funnelNode('ICP Events', autoEvents, null, '#d97757', '⚡')
      +funnelArrow(pct(autoCompanies, autoEvents))
      +funnelNode('Companies', autoCompanies, pct(autoCompanies,autoEvents)+' conv', '#16a34a', '🏢')
      +funnelArrow(pct(autoContactsAll, autoCompanies))
      +funnelNode('Contacts', autoContactsAll, pct(autoContactsAll,autoCompanies)+' conv', '#2563eb', '👤')
      +funnelArrow(pct(autoEnriched, autoContactsAll))
      +funnelNode('Enriched', autoEnriched, pct(autoEnriched,autoContactsAll)+' conv', '#7c3aed', '✉')
      +funnelArrow(pct(autoPushed, autoEnriched))
      +funnelNode('Pushed', autoPushed, pct(autoPushed,autoEnriched)+' conv', '#d97706', '📤')
      +funnelArrow(pct(autoReplied, autoPushed))
      +funnelNode('Replied', autoReplied, pct(autoReplied,autoPushed)+' rate', '#16a34a', '💬')
    +'</div>'
  +'</div>'

  +'<div style="display:grid;grid-template-columns:1fr 1.6fr;gap:16px">'

    +'<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px">'
      +'<div style="font-size:13px;font-weight:700;color:var(--text);margin-bottom:14px">Pipeline health</div>'
      +'<div style="display:flex;flex-direction:column;gap:7px">'
        +pipeStep(1,'Parallel AI signals',activeSignals>0,activeSignals,'#d97757')
        +pipeStep(2,'GPT qualifier',hasOAIKey&&hasQualifier,hasQualifier?qualified+' matched':null,'#7c3aed')
        +pipeStep(3,'Crustdata enrichment',hasCrustKey,superEnriched+' companies','#16a34a')
        +pipeStep(4,'Contact enrichment',totalContacts>0,totalContacts+' found','#2563eb')
        +pipeStep(5,'Email enrichment',withEmail>0,withEmail+' with email','#d97706')
        +pipeStep(6,'Smartlead sending',sentContacts>0,sentContacts+' sent','#d97757')
        +pipeStep(7,'LinkedIn (HeyReach)',liPushed>0,liPushed+' pushed','#0a66c2')
        +pipeStep(7,'Replies',replies>0,replies+' replies','#16a34a')
      +'</div>'
    +'</div>'

    +'<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px;display:flex;flex-direction:column">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">'
        +'<div style="font-size:13px;font-weight:700;color:var(--text)">Recent signal events</div>'
        +'<button onclick="showPage(\'events\',document.querySelector(\'[onclick*=events]\'))" style="font-size:11px;font-family:var(--mono);color:var(--accent);background:none;border:none;cursor:pointer">View all →</button>'
      +'</div>'
      +(recentEvs.length?recentEvs.map(function(e){
        var ql=e.qualified==='yes'?'<span style="font-size:9px;padding:1px 6px;border-radius:3px;background:rgba(22,163,74,0.1);color:var(--green);border:1px solid rgba(22,163,74,0.2);font-family:var(--mono);font-weight:600;flex-shrink:0">✓ ICP</span>'
          :e.qualified==='no'?'<span style="font-size:9px;padding:1px 6px;border-radius:3px;background:var(--surface2);color:var(--text3);border:1px solid var(--border);font-family:var(--mono);flex-shrink:0">✗</span>'
          :'';
        var cn=e.company_name||'';
        return '<div style="display:flex;align-items:flex-start;gap:10px;padding:10px 0;border-bottom:1px solid var(--border)">'
          +'<div style="width:6px;height:6px;border-radius:50%;background:var(--accent);flex-shrink:0;margin-top:5px"></div>'
          +'<div style="flex:1;min-width:0">'
            +'<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">'
              +'<span style="font-size:11px;font-weight:600;color:var(--accent);font-family:var(--mono)">'+esc(e.signal_name||'')+'</span>'
              +ql
              +(cn?'<span style="font-size:10px;font-weight:600;color:var(--text)">'+esc(cn)+'</span>':'')
            +'</div>'
            +'<div style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc((e.output||'').slice(0,90))+'…</div>'
          +'</div>'
          +'<div style="font-size:10px;font-family:var(--mono);color:var(--text3);flex-shrink:0">'+istDate(e.event_date)+'</div>'
        +'</div>';
      }).join('')
      :'<div style="text-align:center;padding:32px 0;color:var(--text3);font-size:13px">No events yet — poll your signals to get started.</div>')
    +'</div>'

  +'</div>';

  el.innerHTML=html;
}

function greeting(){
  var h=new Date().getHours();
  return h<12?'Good morning':h<17?'Good afternoon':'Good evening';
}

function updateMetrics(){
  document.getElementById('m-total').textContent=DB.signals.length;
  document.getElementById('m-active').textContent=DB.signals.filter(s=>s.status==='active').length;
  const td=new Date().toISOString().split('T')[0];
  document.getElementById('m-today').textContent=DB.events.filter(e=>e.type==='event'&&e.fetched_at&&e.fetched_at.startsWith(td)).length;
  document.getElementById('m-keys').textContent=DB.keys.filter(k=>!k.exhausted).length;
  if(DB.meta.lastPoll) document.getElementById('last-poll').textContent='polled '+istTime(DB.meta.lastPoll);
}

function loadSettingsUI(){
  document.getElementById('s-poll-interval').value=DB.settings.pollInterval||30;
  document.getElementById('s-lookback').value=DB.settings.lookback||'3d';
  var _storedProxy=localStorage.getItem('dmand_proxy_url')||(DB.settings&&DB.settings.proxyUrl)||'http://localhost:8765';
  document.getElementById('s-proxy-url').value=_storedProxy;
  if(_storedProxy&&_storedProxy!=='http://localhost:8765'){ PROXY=_storedProxy; }
}

function saveProxyUrl(){
  var val=(document.getElementById('s-proxy-url').value||'').trim().replace(/\/$/,'');
  if(!val){showAlert('Enter a proxy URL.','error');return;}
  localStorage.setItem('dmand_proxy_url',val);
  DB.settings.proxyUrl=val; // persist to KV too
  save();
  PROXY=val;
  showAlert('Proxy URL saved: '+val,'success');
  document.getElementById('proxy-test-result').textContent='';
  checkProxy();
}

async function testProxyConnection(){
  var val=(document.getElementById('s-proxy-url').value||'').trim().replace(/\/$/,'');
  if(!val){showAlert('Enter a proxy URL first.','error');return;}
  var el=document.getElementById('proxy-test-result');
  el.textContent='Testing...'; el.style.color='var(--text3)';
  try{
    var r=await fetch(val+'/v1alpha/monitors',{method:'OPTIONS',signal:AbortSignal.timeout(5000)});
    el.textContent='Connected! HTTP '+r.status;
    el.style.color='var(--green)';
  }catch(e){
    el.textContent='Failed: '+e.message;
    el.style.color='var(--red)';
  }
}

function savePollSettings(){
  DB.settings.pollInterval=parseInt(document.getElementById('s-poll-interval').value)||30;
  DB.settings.lookback=document.getElementById('s-lookback').value;
  startPollTimer(); save(); showAlert('Poll settings saved.','success');
}
function clearEvents(){ if(!confirm('Clear all events?')) return; DB.events=[]; saveAndSync(); renderEvents(); updateMetrics(); }

function updateEventSelection(){
  var cbs=document.querySelectorAll('.event-row-cb:checked');
  var btn=document.getElementById('delete-selected-events-btn');
  var ct=document.getElementById('selected-events-count');
  if(btn) btn.style.display=cbs.length>0?'':'none';
  if(ct) ct.textContent=cbs.length;
}

function deleteSelectedEvents(){
  var cbs=document.querySelectorAll('.event-row-cb:checked');
  var eids=Array.from(cbs).map(function(cb){return cb.dataset.eid;});
  if(!eids.length) return;
  if(!confirm('Delete '+eids.length+' selected event(s)?')) return;
  DB.events=DB.events.filter(function(e){return eids.indexOf(e.event_id)<0;});
  saveAndSync(); renderEvents(); updateMetrics();
  updateEventSelection();
  showAlert(eids.length+' event(s) deleted.','success',3000);
}
function clearResultsData(){
  if(!confirm('Clear all events, companies and contacts?\n\nYour signals, API keys, ICP personas, campaigns and settings will be kept.\nParallel will re-deliver events on the next poll.')) return;
  DB.events    = [];
  DB.companies = [];
  DB.contacts  = [];
  saveAndSync();
  renderEvents(); renderContacts(); renderCompanies(); updateMetrics();
  showAlert('✓ Events, companies and contacts cleared. Hit Poll now to re-collect from Parallel.', 'success', 8000);
  log('🗑 Results cleared — events/companies/contacts wiped. Signals and keys intact.', 'amber');
}

function clearAllData(){
  if(!confirm('Delete ALL data?')) return;
  DB={signals:[],events:[],keys:[],personas:[],contacts:[],companies:[],heyreachKeys:[],hrCampaigns:[],autoboundKeys:[],settings:{pollInterval:30,lookback:'3d'},outreach:{smartleadKey:'',campaignSignalId:null,campaignIcebreakerIdId:null,companyBrief:'',valueProp:'',painPoints:'',cta:'Book a 15-min call'},campaigns:[],meta:{activeKeyIdx:0,lastPoll:null}};
  saveAndSync(); renderAll();
}

async function enrichSingleEmail(contactId){
  const c=DB.contacts.find(x=>x.id===contactId);
  if(!c){return;}
  if(!c.linkedin&&!c.apollo_id){
    log('Email enrich: no LinkedIn URL or Apollo ID for '+c.name,'amber');
    return;
  }
  if(c.email_status==='enriching') return;
  if(c.business_email&&c.business_email!=='N/A'&&c.business_email.trim()) return;

  log('── Email enrichment: '+c.name+' | apollo_id='+(c.apollo_id?'✓':'✗')+' | linkedin='+(c.linkedin?'✓':'✗'),'gray');
  c.email_status='enriching';
  renderContacts(false);

  var apolloKey=DB.settings&&DB.settings.apolloKey;
  if(apolloKey&&(c.linkedin||c.apollo_id)){
    try{
      log('Email enrich [Apollo]: trying for '+c.name,'gray');
      var apolloParams=new URLSearchParams();
      if(c.linkedin) apolloParams.set('linkedin_url',c.linkedin);
      if(c.apollo_id) apolloParams.set('id',c.apollo_id);
      apolloParams.set('reveal_personal_emails','false'); // business email only, no extra credits
      var ar;
      try{
        ar=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/people/match?'+apolloParams.toString(),{
          method:'POST',
          headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
          body:'{}',
          signal:AbortSignal.timeout(15000)
        });
      }catch(matchErr){
        ar=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/people/match?'+apolloParams.toString(),{
          method:'POST',
          headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json','Accept':'application/json','Cache-Control':'no-cache'},
          body:'{}',
          signal:AbortSignal.timeout(15000)
        });
      }
      var atxt=await ar.text();
      log('Apollo email enrich: HTTP '+ar.status+' '+atxt.slice(0,150),(ar.ok?'green':'amber'));
      if(ar.ok){
        var ad=JSON.parse(atxt);
        var person=ad.person||{};
        var email=person.email||'';
        var emailStatus=person.email_status||'';
        var verifiedEmail=email&&email.trim()&&email!=='N/A'&&emailStatus==='verified';
        var hasAnyEmail=email&&email.trim()&&email!=='N/A'&&emailStatus!=='unavailable';
        if(verifiedEmail){
          c.business_email=email.trim();
          c.email_source='apollo';
          c.email_status='done';
          if(person.name&&!c.name) c.name=person.name;
          if(person.title&&!c.title) c.title=person.title;
          if(person.linkedin_url&&!c.linkedin) c.linkedin=person.linkedin_url;
          if(person.organization&&person.organization.name&&!c.company) c.company=person.organization.name;
          log('Email enrich [Apollo]: ✓ found '+email+' (verified) for '+c.name,'green');
          save(); renderContacts(false);
          if(getActiveAutoboundKey()&&!c.autobound_insights){
            setTimeout(function(){triggerInsightsForContact(c);},2000);
          }
          return;
        } else if(hasAnyEmail){
          log('Email enrich [Apollo]: email found but not verified ('+emailStatus+') — sending to FullEnrich','amber');
          if(person.id&&!c.apollo_id) c.apollo_id=person.id;
          if(person.linkedin_url&&!c.linkedin) c.linkedin=person.linkedin_url;
          save();
        } else {
          log('Email enrich [Apollo]: no email found ('+emailStatus+') — falling back to FullEnrich','amber');
        }
      } else {
        log('Email enrich [Apollo]: HTTP '+ar.status+' — falling back to FullEnrich','amber');
      }
    }catch(e){
      log('Email enrich [Apollo] error: '+e.message+' — falling back to FullEnrich','amber');
    }
  }

  const feKey=DB.settings&&DB.settings.fullenrichKey;
  if(!feKey){
    log('Email enrich: no Apollo email found and no FullEnrich key set — trying Autobound+HeyReach with LinkedIn','amber');
    c.email_status='no_email';
    c.business_email='N/A';
    save(); renderContacts(false);
    if(c.linkedin&&getActiveAutoboundKey()&&!c.autobound_insights){
      setTimeout(function(){triggerInsightsForContact(c);},1000);
    }
    return;
  }
  if(!c.linkedin){
    log('Email enrich [FullEnrich]: no LinkedIn URL for '+c.name+' — cannot enrich','amber');
    c.email_status='no_email';
    save(); renderContacts(false);
    return;
  }
  try{
    log('Email enrich [FullEnrich]: trying for '+c.name,'gray');
    const fePayload={requests:[{linkedin:c.linkedin}]};
    const fr=await fetch(PROXY+'/fullenrich/api/requests',{
      method:'POST',
      headers:{'api-key':feKey,'Content-Type':'application/json'},
      body:JSON.stringify(fePayload),
      signal:AbortSignal.timeout(30000)
    });
    const ftxt=await fr.text();
    log('FullEnrich: HTTP '+fr.status+' '+ftxt.slice(0,150),(fr.ok?'green':'red'));
    if(!fr.ok){
      c.email_status='error'; save(); renderContacts(false); return;
    }
    const fd=JSON.parse(ftxt);
    const result=(fd.results||fd.data||[fd])[0]||{};
    const bizEmail=result.work_email||result.professional_email||result.email||'';
    const persEmail=result.personal_email||'';
    const finalEmail=bizEmail||persEmail||'';
    if(finalEmail&&finalEmail!=='N/A'){
      c.business_email=finalEmail;
      c.personal_email=persEmail||undefined;
      c.email_source='fullenrich';
      c.email_status='done';
      log('Email enrich [FullEnrich]: ✓ found '+finalEmail+' for '+c.name,'green');
    } else {
      c.business_email='N/A';
      c.email_status='done';
      log('Email enrich [FullEnrich]: no email found for '+c.name,'amber');
    }
  }catch(e){
    log('Email enrich [FullEnrich] error: '+e.message,'red');
    c.email_status='error';
  }
  save(); renderContacts(false);
  if(!c.autobound_insights||c.insights_status==='skipped'){
    if((c.business_email&&c.business_email!=='N/A')||c.linkedin){
      setTimeout(function(){triggerInsightsForContact(c);},2000);
    }
  }
}

async function getInsightsSingle(contactId){
  const c=DB.contacts.find(x=>x.id===contactId);
  if(!c) return;
  if(!getActiveAutoboundKey()){ showAlert('Add Autobound API keys in the Keys tab first.','error'); return; }
  if(c.insights_status==='loading') return;
  c.insights_status=null;
  c.autobound_insights=[];
  await triggerInsightsForContact(c);
}

function addAutoboundKey(){
  const val=document.getElementById('new-ab-key').value.trim();
  const label=document.getElementById('new-ab-label').value.trim()||('Account #'+(DB.autoboundKeys.length+1));
  if(!val){showAlert('Enter an Autobound API key.','error');return;}
  if(!DB.autoboundKeys) DB.autoboundKeys=[];
  if(DB.autoboundKeys.length>=10){showAlert('Maximum 10 Autobound keys.','warning');return;}
  DB.autoboundKeys.push({id:'abk_'+Date.now(),value:val,label,used:0,exhausted:false});
  document.getElementById('new-ab-key').value='';
  document.getElementById('new-ab-label').value='';
  save();loadAutoboundKeysUI();
  showAlert('Autobound key "'+label+'" added.','success');
}
function removeAutoboundKey(id){
  DB.autoboundKeys=DB.autoboundKeys.filter(k=>k.id!==id);
  save();loadAutoboundKeysUI();
}
function getActiveAutoboundKey(){
  if(!DB.autoboundKeys||!DB.autoboundKeys.length) return null;
  const active=DB.autoboundKeys.filter(k=>!k.exhausted);
  if(!active.length) return null;
  const totalUsed=DB.autoboundKeys.reduce((s,k)=>s+(k.used||0),0);
  return active[totalUsed%active.length];
}
function loadAutoboundKeysUI(){
  if(!DB.autoboundKeys) DB.autoboundKeys=[];
  const el=document.getElementById('autobound-key-list');
  if(!el) return;
  if(!DB.autoboundKeys.length){el.innerHTML='<div style="font-size:11px;color:var(--text3);font-family:var(--mono)">No Autobound keys added yet.</div>';return;}
  el.innerHTML=DB.autoboundKeys.map(function(k){
    const pct=Math.min(100,Math.round((k.used||0)/150*100));
    const col=pct>85?'var(--red)':pct>60?'var(--amber)':'var(--accent)';
    return '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:6px">'
      +'<div style="width:8px;height:8px;border-radius:50%;background:'+(k.exhausted?'var(--red)':col)+';flex-shrink:0"></div>'
      +'<div style="flex:1;min-width:0">'
      +'<div style="font-size:12px;font-weight:600">'+esc(k.label)+'<span style="font-size:10px;font-family:var(--mono);color:var(--text3);margin-left:8px">'+k.value.slice(0,10)+'…</span></div>'
      +'<div style="font-size:10px;font-family:var(--mono);color:var(--text3);margin-top:2px">'+(k.used||0)+'/150 credits'+(k.exhausted?' · exhausted':'')+'</div>'
      +'</div>'
      +'<button class="btn btn-sm btn-danger" onclick="removeAutoboundKey(\''+k.id+'\')">Remove</button>'
      +'</div>';
  }).join('');
  const st=document.getElementById('ab-status');
  if(st) st.textContent=DB.autoboundKeys.filter(k=>!k.exhausted).length+' key(s) active';
}

const INSIGHT_CUTOFF_DAYS=90;
function stripHtml(s){ return (s||'').replace(/<[^>]+>/g,' ').replace(/ +/g,' ').trim(); }

function isInsightRecent(ins){ return true; }
function insightTypeAllowed(ins){ return true; }

async function fetchAutoboundInsights(contact){
  const key=getActiveAutoboundKey(); if(!key) return null;
  const biz=contact.business_email&&contact.business_email!=='N/A'?contact.business_email:'';
  const email=biz;
  const identifier=email?{contactEmail:email}:contact.linkedin?{contactLinkedinUrl:contact.linkedin}:null;
  if(!identifier){log('Autobound: no identifier for '+contact.name,'gray');return null;}
  log('Autobound: fetching insights for '+contact.name+' via '+(identifier.contactEmail||identifier.contactLinkedinUrl||'').slice(0,50),'blue');
  try{
    const res=await fetch(PROXY+'/autobound/api/external/generate-insights/v1.4',{
      method:'POST',
      headers:{'X-API-KEY':key.value,'Content-Type':'application/json','Accept':'application/json'},
      body:JSON.stringify(identifier)
    });
    const txt=await res.text();
    log('Autobound HTTP '+res.status+' — '+txt.slice(0,100),'blue');
    key.used=(key.used||0)+1;
    if(key.used>=150){key.exhausted=true;showAlert('Autobound key "'+key.label+'" exhausted — switching to next.','warning');}
    save(); loadAutoboundKeysUI();
    if(!res.ok){log('Autobound error: '+txt.slice(0,200),'red');return null;}
    const data=JSON.parse(txt);
    const all=data.insights||[];
    const recent=all.filter(isInsightRecent);
    if(recent.length){
      log('Autobound: ✓ '+contact.name+' | '+recent.length+' fresh insights ('+all.length+' total)','green');
    } else {
      log('Autobound: ⚠ '+contact.name+' | '+all.length+' insights found but 0 within 90 days','amber');
    }
    return recent;
  }catch(e){log('Autobound error ['+contact.name+']: '+e.message,'red');return null;}
}

async function triggerInsightsForContact(contact){
  if(contact.insights_status==='done'||contact.insights_status==='loading') return;
  var hasAutoboundKey=!!getActiveAutoboundKey();
  log('── Insights+Push chain: '+contact.name+' ('+( contact.source_type||'?')+') | email='+(contact.business_email&&contact.business_email!=='N/A'?'✓':'✗')+' | linkedin='+(contact.linkedin?'✓':'✗')+' | autobound='+(hasAutoboundKey?'✓':'✗ skipping'),'blue');
  if(hasAutoboundKey){
    contact.insights_status='loading'; save();
    if(insightsPanelContactId===contact.id) renderInsightsBody(contact);
    const insights=await fetchAutoboundInsights(contact);
    contact.autobound_insights=insights||[];
    contact.insights_status='done';
    contact.insights_fetched_at=new Date().toISOString();
    save();
    if(insightsPanelContactId===contact.id) renderInsightsBody(contact);
    renderContacts(false);
  } else {
    contact.insights_status='skipped';
    contact.autobound_insights=[];
    save();
  }
  var _shouldAutoPush = contact.source_type==='automated';
  if(!_shouldAutoPush && contact.signal_id){
    var _sig=DB.signals.find(function(s){return s.id===contact.signal_id;});
    if(_sig&&_sig.icp_targets&&_sig.icp_targets.length){
      var _t=_sig.icp_targets.find(function(t){return t.persona_id===contact.icp_persona_id||t.persona_name===contact.icp_persona_name;})||_sig.icp_targets[0];
      if(_t&&(_t.campaign_id||_t.hr_campaign_id)) _shouldAutoPush=true;
    }
  }
  if(_shouldAutoPush){
    await autoGenerateAndPushContact(contact);
  }
}

let insightsPanelContactId=null;
function openInsightsPanel(contactId){
  const c=DB.contacts.find(function(x){return x.id===contactId;}); if(!c) return;
  insightsPanelContactId=contactId;
  document.getElementById('insights-contact-name').textContent=c.name||'Unknown';
  document.getElementById('insights-contact-meta').textContent=(c.title||'')+(c.company?' · '+c.company:'');
  document.getElementById('insights-panel').style.right='0';
  document.getElementById('insights-overlay').style.display='block';
  renderInsightsBody(c);
}
function closeInsightsPanel(){
  document.getElementById('insights-panel').style.right='-440px';
  document.getElementById('insights-overlay').style.display='none';
  insightsPanelContactId=null;
}

function renderInsightsBody(c){
  const el=document.getElementById('insights-body'); if(!el) return;
  if(c.insights_status==='loading'){
    el.innerHTML='<div style="text-align:center;padding:40px 0"><div class="spinner" style="margin:0 auto 12px;border-top-color:var(--accent)"></div><div style="font-size:12px;font-family:var(--mono);color:var(--text3)">Fetching insights…</div></div>';
    return;
  }
  const ins=c.autobound_insights||[];
  if(!ins.length){
    el.innerHTML='<div class="empty" style="margin-top:40px"><div class="empty-icon">◎</div>'
      +'<div class="empty-title">'+(c.insights_status==='done'?'No insights found':'No insights yet')+'</div>'
      +'<div class="empty-sub">'+(c.insights_status==='done'?'Autobound returned no signals':'Click 💡 get to fetch')+'</div></div>';
    return;
  }
  let html='<div style="font-size:11px;font-family:var(--mono);color:var(--text3);margin-bottom:14px">'
    +ins.length+' insight(s) · '+istDate(c.insights_fetched_at)+'</div>';
  ins.forEach(function(i){ html+=renderInsightCard(i); });
  el.innerHTML=html;
}

function renderInsightCard(ins){
  const v=ins.variables||{};
  const type=(ins.type||'').toLowerCase();
  const sub=ins.subType||ins.sub_type||'';
  const subLower=sub.toLowerCase();

  const typeColors={'linkedin':'#0a66c2','socialmedia':'#0a66c2','twitter':'#1da1f2',
    'podcast':'#9b59b6','youtube':'#ff0000','news':'#3498db','jobopening':'#9b59b6',
    'workhistory':'#7f8c8d','businessmodel':'#27ae60','reddit':'#ff4500'};
  const badgeColor=typeColors[type]||'var(--text3)';

  const subLabel=sub.replace(/^socialMedia/,'').replace(/([A-Z])/g,' $1').trim();

  const date=(v.postedDate||v.publishedDate||v.publishDate||v.postDate||v.episodeDate||v.insightFoundAt||ins.detected_at||'').split('T')[0];

  const url=v.postUrl||v.url||v.podcastEpisodeUrl||v.episodeUrl||v.insightUrl||v.articleUrl||(v.website?'https://'+v.website:'')||'';

  const source=v.companyName||v.podcastName||v.showName||v.insightArticleSource||v.source||'';

  var stats='';
  if(v.numberOfLikes||v.numberOfComments||v.numberOfReposts||v.numberOfReshares){
    stats='<div style="display:flex;gap:10px;margin-top:6px;font-size:10px;font-family:var(--mono);color:var(--text3)">'
      +(v.numberOfLikes?'<span>👍 '+v.numberOfLikes+'</span>':'')
      +(v.numberOfComments?'<span>💬 '+v.numberOfComments+'</span>':'')
      +((v.numberOfReposts||v.numberOfReshares)?'<span>🔁 '+(v.numberOfReposts||v.numberOfReshares)+'</span>':'')
      +'</div>';
  }

  const CONTENT_FIELDS=[
    'postText','first500CharactersOfPost',
    'aboutMe','jobDescription',
    'companyDescription',
    'Takeaway','What open positions signal',
    'yearsAtCompany','yearsWorked','tenure',
    'workHistoryDescription',
    'podcastEpisodeSummary','episodeSummary','summary',
    'insightBody','body','content','description','snippet','text'
  ];
  var mainText='';
  for(var i=0;i<CONTENT_FIELDS.length;i++){
    var val=v[CONTENT_FIELDS[i]];
    if(val===undefined||val===null) continue;
    if(typeof val==='number'&&(CONTENT_FIELDS[i]==='yearsAtCompany'||CONTENT_FIELDS[i]==='yearsWorked'||CONTENT_FIELDS[i]==='tenure')){
      mainText=val+' year'+(val!==1?'s':'')+' at '+(v.companyName||'this company');
      break;
    }
    if(typeof val==='string'&&val.trim().length>3){
      mainText=val.trim(); break;
    }
  }

  const STRUCT_FIELDS=['Department Breakdown','Seniority Breakdown','Location Breakdown',
    'Contract Breakdown','Number of Employees','Number of Open Roles','Hiring Velocity (%)'];
  var structRows='';
  STRUCT_FIELDS.forEach(function(k){
    var val=v[k];
    if(val===undefined||val===null||val==='') return;
    structRows+='<div style="display:flex;gap:8px;padding:4px 0;border-bottom:1px solid var(--surface);align-items:flex-start">'
      +'<span style="font-size:10px;font-family:var(--mono);color:var(--text3);min-width:110px;flex-shrink:0;padding-top:1px">'+esc(k)+'</span>'
      +'<span style="font-size:11px;color:var(--text2);line-height:1.5">'+esc(stripHtml(String(val)).slice(0,200))+'</span>'
      +'</div>';
  });

  var html='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:10px 12px;margin-bottom:8px">';

  html+='<div style="display:flex;align-items:center;justify-content:space-between;gap:6px;margin-bottom:6px;flex-wrap:wrap">'
    +'<div style="display:flex;align-items:center;gap:6px">'
    +'<span style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;padding:2px 6px;border-radius:3px;background:'+badgeColor+'22;color:'+badgeColor+'">'+esc(type)+'</span>'
    +(subLabel?'<span style="font-size:10px;font-family:var(--mono);color:var(--text3)">'+esc(subLabel)+'</span>':'')
    +'</div>'
    +(date?'<span style="font-size:10px;font-family:var(--mono);color:var(--text3)">'+esc(date)+'</span>':'')
    +'</div>';

  if(source) html+='<div style="font-size:11px;font-weight:500;color:var(--text2);margin-bottom:5px">'+esc(source)+'</div>';

  const insName=ins.name||'';
  if(insName) html+='<div style="font-size:12px;font-weight:700;color:var(--text);margin-bottom:6px">'+esc(insName)+'</div>';

  if(mainText){
    const cleaned=stripHtml(mainText);
    html+='<div style="font-size:11px;color:var(--text2);line-height:1.65;white-space:pre-wrap;word-break:break-word">'+esc(cleaned.slice(0,600))+(cleaned.length>600?'\n…':'')+'</div>';
  }

  html+=stats;

  if(structRows) html+='<div style="margin-top:8px">'+structRows+'</div>';

  if(url) html+='<div style="margin-top:8px"><a href="'+esc(url)+'" target="_blank" style="font-size:10px;font-family:var(--mono);color:var(--blue);text-decoration:none">↗ View source</a></div>';

  html+='</div>';
  return html;
}

function saveSmartleadKey(){
  const val=document.getElementById('smartlead-key').value.trim();
  if(!val){showAlert('Enter your Smartlead API key.','error');return;}
  if(!DB.outreach) DB.outreach={};
  DB.outreach.smartleadKey=val;
  DB.outreach.slTimezone=document.getElementById('sl-timezone').value||'Asia/Kolkata';
  DB.outreach.slHourStart=parseInt(document.getElementById('sl-hour-start').value)||9;
  DB.outreach.slHourEnd=parseInt(document.getElementById('sl-hour-end').value)||18;
  DB.outreach.slMaxPerDay=parseInt(document.getElementById('sl-max-per-day').value)||20;
  save();
  document.getElementById('sl-status').textContent='Saved ✓';
  showAlert('Smartlead key saved. Click "Setup campaigns" to create the 2 campaigns.','success');
}

function loadSmartleadKeyUI(){
  if(!DB.outreach) return;
  const el=document.getElementById('smartlead-key');
  if(el&&DB.outreach.smartleadKey) el.value=DB.outreach.smartleadKey;
  const tz=document.getElementById('sl-timezone');
  if(tz&&DB.outreach.slTimezone) tz.value=DB.outreach.slTimezone;
  const hs=document.getElementById('sl-hour-start');
  if(hs&&DB.outreach.slHourStart!=null) hs.value=DB.outreach.slHourStart;
  const he=document.getElementById('sl-hour-end');
  if(he&&DB.outreach.slHourEnd!=null) he.value=DB.outreach.slHourEnd;
  const mp=document.getElementById('sl-max-per-day');
  if(mp&&DB.outreach.slMaxPerDay!=null) mp.value=DB.outreach.slMaxPerDay;
  updateCampaignStatusUI();
}

function saveOutreachSettings(){
  if(!DB.outreach) DB.outreach={};
  DB.outreach.companyBrief=document.getElementById('out-company-brief').value.trim();
  DB.outreach.valueProp=document.getElementById('out-value-prop').value.trim();
  DB.outreach.painPoints=document.getElementById('out-pain-points').value.trim();
  DB.outreach.cta=document.getElementById('out-cta').value.trim()||'Book a 15-min call';
  save();
  const st=document.getElementById('out-status');
  if(st){st.textContent='Saved ✓';setTimeout(function(){st.textContent='';},2000);}
  showAlert('Outreach settings saved.','success');
}

function saveQualifierPrompt(){
  var prompt=(document.getElementById('icp-qualifier-prompt').value||'').trim();
  if(!prompt){showAlert('Enter a qualifier prompt.','error');return;}
  DB.settings.qualifierPrompt=prompt;
  save();
  showAlert('Qualifier prompt saved.','success');
}

function loadQualifierPromptUI(){
  var el=document.getElementById('icp-qualifier-prompt');
  if(el&&DB.settings.qualifierPrompt) el.value=DB.settings.qualifierPrompt;
}

async function qualifyEvent(ev){
  var oaiKey=DB.settings.oaiKey;
  if(!oaiKey) return null;
  var prompt=DB.settings.qualifierPrompt;
  if(!prompt) return null;
  var userMsg='Event:\n'+ev.output+'\n\n'+prompt+'\n\nAnswer only Yes or No.';
  try{
    var r=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+oaiKey},
      body:JSON.stringify({
        model:DB.settings.oaiModel||'gpt-4o-mini',
        max_tokens:5,
        messages:[{role:'user',content:userMsg}]
      })
    });
    var data=await r.json();
    var ans=(data.choices[0].message.content||'').trim().toLowerCase();
    var isYes=ans.startsWith('yes');
    log('Qualifier ['+ev.signal_name.slice(0,20)+']: '+(isYes?'YES — enrolling':'NO — skip'),'blue');
    return isYes;
  }catch(e){
    log('Qualifier error: '+e.message,'red');
    return null;
  }
}

async function runQualifierOnAll(){
  var oaiKey=DB.settings.oaiKey;
  if(!oaiKey){showAlert('Add OpenAI API key in Keys tab first.','error');return;}
  var prompt=DB.settings.qualifierPrompt;
  if(!prompt){showAlert('Save a qualifier prompt first.','error');return;}
  var events=DB.events.filter(function(e){
    return e.type==='event'
      && e.qualified===undefined
      && e.enrichment&&e.enrichment.status==='done';  // only after Parallel enrichment
  });
  if(!events.length){
    showAlert('No enriched-but-unqualified events found. Wait for Parallel enrichment to complete first.','info');
    return;
  }
  var btn=document.getElementById('btn-qualify-all');
  var st=document.getElementById('qualifier-status');
  if(btn){btn.disabled=true;btn.textContent='Qualifying...';}
  var yes=0,no=0;
  for(var i=0;i<events.length;i++){
    var ev=events[i];
    if(st) st.textContent='Checking '+(i+1)+'/'+events.length+'...';
    var result=await qualifyEvent(ev);
    if(result===null) continue;
    ev.qualified=result?'yes':'no';
    if(result){
      yes++;
      if(!ev.superEnrichment||ev.superEnrichment.status!=='done'){
        log('Auto Super Enrich: '+ev.signal_name,'green');
        setTimeout(function(eid){return function(){superEnrichEvent(eid);};}(ev.event_id), i*2000);
      }
    } else { no++; }
  }
  save(); renderEvents();
  if(st) st.textContent='Done: '+yes+' qualified, '+no+' skipped';
  if(btn){btn.disabled=false;btn.textContent='Run on all unqualified events';}
  showAlert(yes+' event(s) qualified and sent to Super Enrich','success');
}

function loadOutreachSettingsUI(){
  if(!DB.outreach) return;
  const f=function(id,val){const el=document.getElementById(id);if(el&&val)el.value=val;};
  f('out-company-brief',DB.outreach.companyBrief);
  f('out-value-prop',DB.outreach.valueProp);
  f('out-pain-points',DB.outreach.painPoints);
  f('out-cta',DB.outreach.cta);
}

function updateCampaignStatusUI(){
  const el=document.getElementById('sl-campaign-status'); if(!el) return;
  if(!DB.outreach) return;
  const s=DB.outreach.campaignSignalId, i=DB.outreach.campaignIcebreakerIdId;
  el.innerHTML=(s?'✅ Signal campaign: ID '+s:'⚪ Signal campaign: not created')+'<br>'
    +(i?'✅ Signal + Icebreaker campaign: ID '+i:'⚪ Signal + Icebreaker campaign: not created');
}

async function initSmartleadCampaigns(){
  const key=DB.outreach&&DB.outreach.smartleadKey;
  if(!key){showAlert('Save your Smartlead API key first.','error');return;}
  const btn=document.getElementById('btn-init-campaigns');
  if(!btn) return;
  const sl=document.getElementById('sl-status');
  if(btn){btn.disabled=true;btn.textContent='Setting up…';}

  const timezone=DB.outreach.slTimezone||'Asia/Kolkata';
  const hourStart=DB.outreach.slHourStart||9;
  const hourEnd=DB.outreach.slHourEnd||18;
  const maxPerDay=DB.outreach.slMaxPerDay||20;
  const fmt=function(h){return h.toString().padStart(2,'0')+':00';};

  const SCHEDULE={timezone:timezone,days_of_the_week:[1,2,3,4,5],
    start_hour:fmt(hourStart),end_hour:fmt(hourEnd),
    min_time_btw_emails:10,max_new_leads_per_day:maxPerDay};

  const SETTINGS={track_open:false,track_click:false,
    stop_lead_settings:'REPLY_TO_AN_EMAIL',
    send_as_plain_text:false,follow_up_percentage:50};

  const SEQUENCE=[
    {seq_number:1,seq_delay_details:{delay_in_days:0},
     variants:[{subject:'{{step1_subject}}',email_body:'{{step1_body}}',variant_label:'A'}]},
    {seq_number:2,seq_delay_details:{delay_in_days:3},
     variants:[{subject:'',email_body:'{{step2_body}}',variant_label:'A'}]},
    {seq_number:3,seq_delay_details:{delay_in_days:3},
     variants:[{subject:'',email_body:'{{step3_body}}',variant_label:'A'}]}
  ];

  try{
    if(sl) sl.textContent='Finding Hitesh mailboxes…';
    const accsRes=await fetch(PROXY+'/smartlead/api/v1/email-accounts?api_key='+encodeURIComponent(key));
    const accsData=await accsRes.json();
    log('Smartlead accounts: '+JSON.stringify(accsData).slice(0,300),'blue');
    const accounts=(Array.isArray(accsData)?accsData:[]).filter(function(a){
      return (a.from_name||a.name||a.email||'').toLowerCase().includes('hitesh');
    });
    if(!accounts.length){showAlert('No mailboxes with "hitesh" found in Smartlead.','error');return;}
    const accountIds=accounts.map(function(a){return a.id;});
    log('Hitesh mailboxes: '+accountIds.join(', '),'green');

    async function createCamp(name){
      const r=await fetch(PROXY+'/smartlead/api/v1/campaigns/create?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({name:name,client_id:null})
      });
      const d=await r.json(); log('Created: '+JSON.stringify(d).slice(0,100),'blue'); return d;
    }

    async function setupCamp(id){
      const r1=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/email-accounts?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({email_account_ids:accountIds})
      });
      log('Mailboxes '+id+': HTTP '+r1.status+' '+JSON.stringify(await r1.json()).slice(0,80),'blue');

      const r2=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/sequences?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify(SEQUENCE)
      });
      log('Sequence '+id+': HTTP '+r2.status+' '+JSON.stringify(await r2.json()).slice(0,80),'blue');

      const r3=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/schedule?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify(SCHEDULE)
      });
      log('Schedule '+id+': HTTP '+r3.status+' '+JSON.stringify(await r3.json()).slice(0,80),'blue');

      const r4=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/settings?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify(SETTINGS)
      });
      log('Settings '+id+': HTTP '+r4.status+' '+JSON.stringify(await r4.json()).slice(0,80),'blue');

      const r5=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/status?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({status:'START'})
      });
      log('Activate '+id+': HTTP '+r5.status+' '+JSON.stringify(await r5.json()).slice(0,80),'green');
    }

    if(sl) sl.textContent='Creating campaigns…';
    const c1=await createCamp('Dmand — Signal Based');
    const c2=await createCamp('Dmand — Signal + Icebreaker');
    const id1=c1.id||c1.campaign_id;
    const id2=c2.id||c2.campaign_id;
    if(!id1||!id2){showAlert('Campaign creation failed — check API key permissions.','error');return;}

    if(sl) sl.textContent='Configuring campaign 1…';
    await setupCamp(id1);
    if(sl) sl.textContent='Configuring campaign 2…';
    await setupCamp(id2);

    DB.outreach.campaignSignalId=id1;
    DB.outreach.campaignIcebreakerIdId=id2;
    save(); updateCampaignStatusUI();

    showAlert('Campaigns configured! '+accounts.length+' mailbox(es), Mon-Fri '+fmt(hourStart)+'-'+fmt(hourEnd)+', Open tracking OFF, Stop on reply ON','success');
    if(sl) sl.textContent=accounts.length+' mailbox(es) - Active';
  }catch(e){
    log('Smartlead setup error: '+e.message,'red');
    showAlert('Smartlead setup error: '+e.message,'error');
  }finally{
    if(btn){btn.disabled=false;btn.textContent='Setup campaigns (auto-create)';}
  }
}
async function reapplySmartleadSettings(){
  const key=DB.outreach&&DB.outreach.smartleadKey;
  if(!key){showAlert('No Smartlead API key saved.','error');return;}
  const id1=DB.outreach.campaignSignalId;
  const id2=DB.outreach.campaignIcebreakerIdId;
  if(!id1||!id2){showAlert('No campaign IDs found. Run "Setup campaigns" first.','error');return;}

  const timezone=DB.outreach.slTimezone||'Asia/Kolkata';
  const hourStart=DB.outreach.slHourStart||9;
  const hourEnd=DB.outreach.slHourEnd||18;
  const maxPerDay=DB.outreach.slMaxPerDay||20;
  const fmt=function(h){return h.toString().padStart(2,'0')+':00';};

  const SCHEDULE={timezone:timezone,days_of_the_week:[1,2,3,4,5],
    start_hour:fmt(hourStart),end_hour:fmt(hourEnd),
    min_time_btw_emails:10,max_new_leads_per_day:maxPerDay};

  const SETTINGS={track_open:false,track_click:false,
    stop_lead_settings:'REPLY_TO_AN_EMAIL',
    send_as_plain_text:false,follow_up_percentage:50};

  const btn=document.getElementById('btn-reapply-settings');
  const sl=document.getElementById('sl-status');
  if(btn){btn.disabled=true;btn.textContent='Applying...';}

  async function applyToId(id){
    var h={'Content-Type':'application/json'};
    var r1=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/schedule?api_key='+encodeURIComponent(key),{method:'POST',headers:h,body:JSON.stringify(SCHEDULE)});
    var d1=await r1.json();
    log('Schedule '+id+': '+r1.status+' '+JSON.stringify(d1).slice(0,100),'blue');
    var r2=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/settings?api_key='+encodeURIComponent(key),{method:'POST',headers:h,body:JSON.stringify(SETTINGS)});
    var d2=await r2.json();
    log('Settings '+id+': '+r2.status+' '+JSON.stringify(d2).slice(0,100),'blue');
    var r3=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+id+'/status?api_key='+encodeURIComponent(key),{method:'POST',headers:h,body:JSON.stringify({status:'START'})});
    var d3=await r3.json();
    log('Activate '+id+': '+r3.status+' '+JSON.stringify(d3).slice(0,100),'green');
  }

  try{
    if(sl) sl.textContent='Applying to campaign '+id1+'…';
    await applyToId(id1);
    if(sl) sl.textContent='Applying to campaign '+id2+'…';
    await applyToId(id2);
    showAlert('Settings applied to both campaigns! Mon-Fri '+fmt(hourStart)+'-'+fmt(hourEnd)+' '+timezone+'. Open tracking: OFF. Stop on reply: ON. Status: ACTIVE. Check Activity log for details.','success');
    if(sl) sl.textContent='Settings applied OK';
  }catch(e){
    log('Re-apply error: '+e.message,'red');
    showAlert('Error: '+e.message,'error');
  }finally{
    if(btn){btn.disabled=false;btn.textContent='Re-apply settings';}
  }
}

let emailModalContactId=null;
let emailModalContactIds=[];
let emailModalType='signal';
let emailModalCampaignId=null;
let emailModalCampaignName='';
let generatedSteps=[];

function openEmailModal(contactId){
  if(!DB.outreach||!DB.outreach.smartleadKey){showAlert('Add your Smartlead API key in the Keys tab first.','error');return;}
  if(!DB.settings.oaiKey){showAlert('Add your OpenAI API key in the Keys tab first.','error');return;}
  const c=DB.contacts.find(function(x){return x.id===contactId;});
  if(!c){return;}
  emailModalContactId=contactId;
  emailModalContactIds=[contactId];
  generatedSteps=[];
  document.getElementById('email-modal-title').textContent='Generate Email — '+esc(c.name||'Contact');
  document.getElementById('email-modal-sub').textContent=(c.title||'')+(c.company?' · '+c.company:'');
  document.getElementById('email-steps-container').innerHTML='<div class="empty" style="margin:24px 0"><div class="empty-icon" style="font-size:24px">✨</div><div class="empty-title" style="font-size:13px">Click generate to create your sequence</div></div>';
  document.getElementById('email-send-row').style.display='none';
  document.getElementById('email-send-status').textContent='';
  setEmailType('signal');
  document.getElementById('email-modal-overlay').style.display='block';
  document.getElementById('email-modal').style.display='block';
  setTimeout(function(){
    var genBtn=document.getElementById('btn-generate-emails');
    if(genBtn&&!genBtn.disabled) genBtn.click();
  },200);
}

function openBulkEmailModal(){
  if(!DB.outreach||!DB.outreach.smartleadKey){showAlert('Add your Smartlead API key in the Keys tab first.','error');return;}
  if(!DB.settings.oaiKey){showAlert('Add your OpenAI API key in the Keys tab first.','error');return;}
  const ids=[...selectedContactIds].filter(function(id){
    const c=DB.contacts.find(function(x){return x.id===id;});
    return c&&(c.business_email&&c.business_email!=='N/A');
  });
  if(!ids.length){showAlert('No selected contacts have emails.','warning');return;}
  emailModalContactId=ids[0];
  emailModalContactIds=ids;
  generatedSteps=[];
  const first=DB.contacts.find(function(x){return x.id===ids[0];});
  document.getElementById('email-modal-title').textContent='Generate Emails — '+ids.length+' contact(s)';
  document.getElementById('email-modal-sub').textContent='Generating for first contact as preview. All '+ids.length+' will be sent.';
  document.getElementById('email-steps-container').innerHTML='<div class="empty" style="margin:24px 0"><div class="empty-icon" style="font-size:24px">✨</div><div class="empty-title" style="font-size:13px">Click generate to preview sequence for '+esc(first?first.name||'contact':'contact')+'</div></div>';
  document.getElementById('email-send-row').style.display='none';
  document.getElementById('email-send-status').textContent='';
  setEmailType('signal');
  document.getElementById('email-modal-overlay').style.display='block';
  document.getElementById('email-modal').style.display='block';
}

function closeEmailModal(){
  document.getElementById('email-modal-overlay').style.display='none';
  document.getElementById('email-modal').style.display='none';
  emailModalContactId=null;
  emailModalCampaignId=null;
  emailModalCampaignName='';
}

function setEmailType(type){
  emailModalType=type;
  const b1=document.getElementById('btn-type-signal');
  const b2=document.getElementById('btn-type-icebreaker');
  if(!b1||!b2) return;
  if(type==='signal'){
    b1.className='btn btn-accent'; b2.className='btn';
  } else {
    b2.className='btn btn-accent'; b1.className='btn';
  }
}

async function generateEmails(){
  const c=DB.contacts.find(function(x){return x.id===emailModalContactId;});
  if(!c) return;
  const oaiKey=DB.settings.oaiKey;
  if(!oaiKey){showAlert('OpenAI key required.','error');return;}

  const camp=DB.campaigns&&DB.campaigns.find(function(x){return String(x.id)===String(emailModalCampaignId);});
  const seqs=(camp&&camp.sequences)||[];
  const stepCount=seqs.length||3;

  const btn=document.getElementById('btn-generate-emails');
  btn.disabled=true; btn.textContent='Writing...';
  document.getElementById('email-steps-container').innerHTML='<div style="text-align:center;padding:30px"><div class="spinner" style="margin:0 auto 10px;border-top-color:var(--accent)"></div><div style="font-size:12px;font-family:var(--mono);color:var(--text3)">GPT writing '+stepCount+'-step sequence for '+esc(c.name||'contact')+'...</div></div>';
  var loadDots=0;
  var loadTimer=setInterval(function(){
    loadDots=(loadDots+1)%4;
    btn.textContent='Writing'+'...'.slice(0,loadDots+1);
  },600);

  const o=DB.outreach||{};
  const signal=c.signal_name||'';
  const event=c.event_brief||'';
  const insights=buildInsightsSummary(c);
  const isIcebreaker=emailModalType==='icebreaker';

  var stepsWithSubject=new Set();
  var campSeqs=(camp&&camp.sequences)||[];
  if(campSeqs.length){
    campSeqs.forEach(function(seq,i){
      var subj=seq.subject||(seq.variants&&seq.variants[0]&&seq.variants[0].subject)||'';
      if(subj&&subj.trim()) stepsWithSubject.add(i); // i=0 = step 1
    });
  } else {
    stepsWithSubject.add(0); // fallback: step 1 has subject
  }

  var jsonSchema='{\n  "step1": {"subject": "...", "body": "..."}';
  for(var i=2;i<=stepCount;i++){
    if(stepsWithSubject.has(i-1)) jsonSchema+=',\n  "step'+i+'": {"subject": "...", "body": "..."}';
    else jsonSchema+=',\n  "step'+i+'": {"body": "..."}';
  }
  jsonSchema+='\n}';

  var varList='subject_1 (step 1 subject), body_1 (step 1 body)';
  for(var i=2;i<=stepCount;i++){
    if(stepsWithSubject.has(i-1)) varList+=', subject_'+i+' (step '+i+' subject), body_'+i+' (step '+i+' body)';
    else varList+=', body_'+i+' (step '+i+' follow-up)';
  }

  var linkInstructions=''; // Plain text emails — no links

  var exRef='';
  if(camp&&camp.example_steps&&camp.example_steps.some(function(s){return s&&(s.subject||s.body);})){
    exRef='\nTone guide (match style, do NOT copy):\n'+camp.example_steps.map(function(s,i){
      if(!s||(!s.subject&&!s.body)) return '';
      return 'Step '+(i+1)+(s.subject?' | Subject: '+s.subject.slice(0,50):'')+(s.body?' | Body: '+s.body.slice(0,80)+'…':'')+(s.cta?' | CTA: '+s.cta:'');
    }).filter(Boolean).join('\n');
  }
  const systemPrompt='B2B cold email writer. Return ONLY valid JSON: '+jsonSchema+'.'
    +'Rules: max 80 words/email, first name only, no sign-off/signature, human tone, no fluff.'
    +' Company: '+(o.companyBrief||'').slice(0,150)+'. Value: '+(o.valueProp||'').slice(0,100)+'. Pain: '+(o.painPoints||'').slice(0,100)+'.'
    +' CTA: '+((camp&&camp.cta)||o.cta||'Book a 15-min call')+'.'
    +exRef
    +linkInstructions;

  const userPrompt=`Write a ${stepCount}-step cold email sequence for this prospect.

Prospect:
- Name: ${c.name||''}
- First name: ${(c.name||'').split(' ')[0]||'there'}
- Title: ${c.title||''}
- Company: ${c.company||''}
- Signal: ${signal}
- Event context: ${event}
${isIcebreaker&&insights?'- Recent activity (use for icebreaker hook):\n'+insights:''}

Email type: ${isIcebreaker?'Signal + Icebreaker — Step 1 opens with a personal hook from their recent activity, then bridges to the signal and value prop':'Signal-based — Step 1 opens with the trigger signal/event, then value prop'}

Return ONLY this JSON structure (${stepCount} steps):
${jsonSchema}`;

  const t0=Date.now();
  log('Email gen: sending to GPT (system='+systemPrompt.length+' chars, user='+userPrompt.length+' chars)','gray');
  try{
    const res=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+oaiKey},
      body:JSON.stringify({
        model:DB.settings.oaiModel||'gpt-4o-mini',
        max_tokens:220*stepCount,
        messages:[{role:'system',content:systemPrompt},{role:'user',content:userPrompt}]
      }),
      signal:AbortSignal.timeout(30000)
    });
    const data=await res.json();
    log('Email gen: GPT responded in '+(Date.now()-t0)+'ms','gray');
    if(!res.ok){showAlert('OpenAI error: '+(data.error&&data.error.message||'unknown'),'error');return;}
    const raw=data.choices[0].message.content.trim();
    const clean=raw.replace(/```json|```/g,'').trim();
    const parsed=JSON.parse(clean);
    generatedSteps=parsed;
    renderEmailSteps(parsed,c.name||'',stepCount);
    document.getElementById('email-send-row').style.display='block';
  }catch(e){
    log('Email generation error: '+e.message,'red');
    document.getElementById('email-steps-container').innerHTML='<div style="color:var(--red);font-size:12px;padding:16px">Error: '+esc(e.message)+'<br><br>Raw response may not be valid JSON. Try again.</div>';
  }finally{
    clearInterval(loadTimer);
    btn.disabled=false; btn.textContent='✨ Regenerate';
  }
}

function buildInsightsSummary(c){
  const ins=c.autobound_insights||[];
  if(!ins.length) return '';
  var lines=[];
  ins.slice(0,5).forEach(function(i){
    const v=i.variables||{};
    const text=v.postText||v.first500CharactersOfPost||v.podcastEpisodeSummary||v.aboutMe||v.summary||'';
    const date=v.postedDate||v.publishedDate||'';
    if(text) lines.push((date?'['+date.split('T')[0]+'] ':'')+(i.name||i.subType||'')+': '+text.slice(0,200));
  });
  return lines.join('\n');
}

function renderEmailSteps(steps,name,stepCount){
  var camp=DB.campaigns&&DB.campaigns.find(function(c){return String(c.id)===String(emailModalCampaignId);});
  var seqs=(camp&&camp.sequences)||[];
  stepCount=stepCount||seqs.length||3;
  var html='';
  function stepCard(num,subj,body){
    var hasSubj=subj!==undefined;
    var seq=seqs[num-1]||{};
    var delay=seq.seq_delay_details&&seq.seq_delay_details.delay_in_days!=null?seq.seq_delay_details.delay_in_days:(num-1)*3;
    var seqSubj=seq.subject||(seq.variants&&seq.variants[0]&&seq.variants[0].subject)||'';
    var isNewThread=!!(seqSubj&&seqSubj.trim());
    var label=num===1?'Initial outreach':(isNewThread?'New thread':'Follow-up #'+(num-1));
    var threadInfo=num===1?'Day 0':(isNewThread?'✉ New thread · Day '+delay:'↩ Same thread · Day '+delay);
    return '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:14px;margin-bottom:12px">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">'
        +'<div style="font-size:11px;font-weight:700;font-family:var(--mono);color:var(--accent)">STEP '+num+' — '+label+'</div>'
        +'<div style="font-size:10px;font-family:var(--mono);color:var(--text3)">'+threadInfo+'</div>'
      +'</div>'
      +(hasSubj?'<div style="margin-bottom:8px"><label style="font-size:10px;font-family:var(--mono);color:var(--text3);display:block;margin-bottom:3px">SUBJECT</label>'
        +'<input class="form-input" style="font-size:12px" id="step'+num+'_subject" value="'+esc(subj)+'"></div>':'')
      +'<div><label style="font-size:10px;font-family:var(--mono);color:var(--text3);display:block;margin-bottom:3px">BODY</label>'
        +'<textarea class="form-input" rows="5" style="font-size:12px;resize:vertical" id="step'+num+'_body">'+esc(body)+'</textarea></div>'
      +'</div>';
  }
  html+=stepCard(1,steps.step1&&steps.step1.subject!=null?steps.step1.subject:'',steps.step1&&steps.step1.body||'');
  for(var i=2;i<=stepCount;i++){
    var stepData=steps['step'+i]||{};
    var seq2=seqs[i-1]||{};
    var seqSubj2=seq2.subject||(seq2.variants&&seq2.variants[0]&&seq2.variants[0].subject)||'';
    var stepIsNewThread=!!(seqSubj2&&seqSubj2.trim());
    var showSubj=stepIsNewThread||stepData.subject!=null;
    html+=stepCard(i,showSubj?(stepData.subject!=null?stepData.subject:''):undefined,stepData.body||'');
  }
  document.getElementById('email-steps-container').innerHTML=html;
}

function unescHtml(s){
  var t=document.createElement('textarea');
  t.innerHTML=s;
  return t.value;
}

async function pushToSmartlead(){
  const key=DB.outreach&&DB.outreach.smartleadKey;
  if(!key){showAlert('No Smartlead key.','error');return;}
  const campaignId=emailModalCampaignId
    ||(emailModalType==='signal'?DB.outreach.campaignSignalId:DB.outreach.campaignIcebreakerIdId);
  if(!campaignId){showAlert('Pick a campaign from the dropdown on the contact row, or set up campaigns in the Campaigns tab.','error');return;}

  const camp2=DB.campaigns&&DB.campaigns.find(function(x){return String(x.id)===String(campaignId);});
  const seqs2=(camp2&&camp2.sequences)||[];
  const stepCount2=seqs2.length||3;

  const s1subj=(document.getElementById('step1_subject')||{}).value||'';
  const s1body=unescHtml((document.getElementById('step1_body')||{}).value||'');
  if(!s1subj||!s1body){showAlert('Step 1 subject and body are required.','error');return;}

  var customFields={job_title:'',linkedin_url:''};
  var mapping2=camp2&&camp2.var_mapping||{};
  var hasMappings=Object.keys(mapping2).length>0;

  function slField(ourVar, defaultName){
    if(hasMappings&&mapping2[ourVar]) return mapping2[ourVar];
    var camp3=DB.campaigns&&DB.campaigns.find(function(c){return String(c.id)===String(campaignId);});
    if(camp3&&camp3.custom_vars){
      var m3=camp3.custom_vars.find(function(v){return v.toLowerCase()===ourVar.toLowerCase();});
      if(m3) return m3;
    }
    return defaultName;
  }
  customFields[slField('subject_1','subject_1')]=s1subj;
  customFields[slField('body_1','body_1')]=s1body.replace(/<[^>]+>/g,' ').replace(/  +/g,' ').trim();

  for(var si=2;si<=stepCount2;si++){
    var stepEl2=document.getElementById('step'+si+'_body');
    var stepBody2=stepEl2?(stepEl2.value||'').replace(/<[^>]+>/g,' ').replace(/  +/g,' ').trim():'';
    var subjEl2=document.getElementById('step'+si+'_subject');
    var stepSubj2=subjEl2?subjEl2.value||'':'';
    customFields[slField('body_'+si,'body_'+si)]=stepBody2;
    if(stepSubj2) customFields[slField('subject_'+si,'subject_'+si)]=stepSubj2;
  }

  const statusEl=document.getElementById('email-send-status');
  const btn=document.querySelector('#email-send-row .btn-accent');
  if(btn){btn.disabled=true;btn.textContent='Pushing…';}

  let sent=0,failed=0;
  for(var i=0;i<emailModalContactIds.length;i++){
    const c=DB.contacts.find(function(x){return x.id===emailModalContactIds[i];});
    if(!c) continue;
    const email=(c.business_email&&c.business_email!=='N/A'?c.business_email:'')||'';
    if(!email) continue;

    var stepFields=Object.assign({},customFields);
    if(emailModalContactIds.length>1){
      const freshSteps=await generateEmailsForContact(c);
      if(freshSteps){
        stepFields.subject_1=freshSteps.step1&&freshSteps.step1.subject||customFields.subject_1;
        stepFields.body_1=freshSteps.step1&&freshSteps.step1.body||customFields.body_1;
        for(var si2=2;si2<=stepCount2;si2++){
          var fk='step'+si2; if(freshSteps[fk]) stepFields['body_'+si2]=freshSteps[fk].body||customFields['body_'+si2];
        }
      }
    } else { stepFields=customFields; }

    if(statusEl) statusEl.textContent='Pushing '+c.name+' ('+( i+1)+'/'+emailModalContactIds.length+')…';

    try{
      const nameParts=(c.name||'').split(' ');
      const payload={
        lead_list:[{
          email:email,
          first_name:nameParts[0]||'',
          last_name:nameParts.slice(1).join(' ')||'',
          company_name:c.company||'',
          custom_fields:Object.assign({},stepFields,{job_title:c.title||'',linkedin_url:c.linkedin||''})
        }],
        settings:{ignore_global_block_list:false,ignore_unsubscribe_list:false,ignore_community_bounce_list:false}
      };
      const r=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+campaignId+'/leads?api_key='+encodeURIComponent(key),{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify(payload)
      });
      const d=await r.json();
      log('Smartlead push '+c.name+': '+JSON.stringify(d).slice(0,100),'green');
      if(d.ok||d.message||d.added_count>=0){
        c.smartlead_sent=emailModalType; c.smartlead_campaign_id=campaignId; sent++;
      } else { failed++; }
    }catch(e){
      log('Smartlead error '+c.name+': '+e.message,'red'); failed++;
    }
  }
  save(); renderContacts(false);
  if(statusEl) statusEl.textContent='Done: '+sent+' pushed'+( failed?' · '+failed+' failed':'');
  if(btn){btn.disabled=false;btn.textContent='🚀 Push to Smartlead';}
  if(sent>0) showAlert(sent+' contact(s) pushed to Smartlead '+emailModalType+' campaign!','success');
}

async function generateEmailsForContact(c){
  var sig=DB.signals.find(function(s){return s.id===c.signal_id;});
  var target=sig&&sig.icp_targets&&sig.icp_targets.find(function(t){
    return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;
  });
  var campId=target&&target.campaign_id;
  var camp=campId&&DB.campaigns&&DB.campaigns.find(function(x){return String(x.id)===String(campId);});
  var mode=emailModalType||'signal';
  var stepCount=(camp&&camp.sequences&&camp.sequences.length)||3;
  return await generateEmailStepsForContact(c, sig||{name:c.signal_name||''}, camp, stepCount, mode);
}

async function refreshCampaigns(){
  var key=DB.outreach&&DB.outreach.smartleadKey;
  if(!key){showAlert('Add Smartlead API key in Keys tab first.','error');return;}
  var el=document.getElementById('campaign-list');
  if(el) el.innerHTML='<div style="text-align:center;padding:30px;font-size:12px;font-family:var(--mono);color:var(--text3)">Loading campaigns from Smartlead...</div>';
  log('Campaigns: fetching from Smartlead...','blue');

  try{
    var r=await fetch(PROXY+'/smartlead/api/v1/campaigns?api_key='+encodeURIComponent(key),{signal:AbortSignal.timeout(15000)});
    var rawText=await r.text();
    log('Campaigns: HTTP '+r.status+' — '+rawText.slice(0,120),'gray');
    if(!r.ok||rawText.trim().startsWith('<')){
      log('Campaigns fetch failed: '+rawText.slice(0,200),'red');
      showAlert('Smartlead error: HTTP '+r.status,'error',0);
      if(el) el.innerHTML='';
      return;
    }
    var data; try{ data=JSON.parse(rawText); }catch(pe){ log('Campaigns JSON parse error: '+pe.message,'red'); showAlert('Invalid response from Smartlead','error'); return; }
    var list=Array.isArray(data)?data:((data&&data.list)||(data&&data.campaigns)||(data&&data.data)||[]);
    if(!list||!list.length){
      log('Campaigns: no campaigns returned. Raw: '+JSON.stringify(data).slice(0,200),'amber');
      showAlert('No campaigns found in Smartlead account.','warning');
      renderCampaignList(); return;
    }
    log('Campaigns: '+list.length+' campaign(s) found','green');
    if(!DB.campaigns) DB.campaigns=[];

    list.forEach(function(sc){
      var existing=DB.campaigns.find(function(c){return String(c.id)===String(sc.id);});
      if(existing){
        existing.name=sc.title||sc.name||existing.name;
        existing.status=sc.status||existing.status;
        existing.lead_count=sc.sent_count||sc.leads_count||sc.total_leads||existing.lead_count||0;
        existing.reply_count=sc.reply_count||existing.reply_count||0;
        log('Campaigns: updated "'+existing.name+'" ('+existing.status+')','gray');
      } else {
        var nc={id:sc.id,name:sc.title||sc.name||('Campaign '+sc.id),
          mode:'',status:sc.status||'ACTIVE',
          created_at:sc.created_at||new Date().toISOString(),
          lead_count:sc.sent_count||sc.leads_count||0,reply_count:sc.reply_count||0,
          sequences:[],schedule:null,custom_vars:[],var_mapping:{},example_steps:[]};
        DB.campaigns.push(nc);
        log('Campaigns: added "'+nc.name+'" ('+nc.status+')','green');
      }
    });

    if(el) el.innerHTML='<div style="text-align:center;padding:30px;font-size:12px;font-family:var(--mono);color:var(--text3)">Fetching sequences & schedules...</div>';
    log('Campaigns: fetching sequences for '+DB.campaigns.length+' campaigns...','blue');

    await Promise.all(DB.campaigns.map(async function(camp){
      try{
        var rs=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+camp.id+'/sequences?api_key='+encodeURIComponent(key),{signal:AbortSignal.timeout(10000)});
        var seqText=await rs.text();
        var seqData; try{ seqData=JSON.parse(seqText); }catch(e){ seqData=[]; }
        camp.sequences=Array.isArray(seqData)?seqData:((seqData&&seqData.data)||(seqData&&seqData.sequences)||[]);
        if(!Array.isArray(camp.sequences)) camp.sequences=[];
        var vars={};
        var nativeVars=['first_name','last_name','company_name','email','phone_number','website','location','linkedin_profile'];
        (camp.sequences||[]).forEach(function(seq){
          var topSubject=seq.subject||'';
          var topBody=seq.email_body||seq.body||'';
          var variants=Array.isArray(seq.variants)?seq.variants:[seq];
          var allText=topSubject+' '+topBody;
          variants.forEach(function(v){
            allText+=' '+(v.email_body||v.body||'')+' '+(v.subject||'');
          });
          var matches=allText.match(/\{\{([^}]+)\}\}/g)||[];
          matches.forEach(function(m){
            var name=m.replace(/\{\{|\}\}/g,'').trim();
            if(name&&!nativeVars.includes(name)) vars[name]=1;
          });
        });
        camp.custom_vars=Object.keys(vars).sort();
        log('Campaigns: "'+camp.name+'" vars detected: ['+camp.custom_vars.join(', ')+']','gray');

        if(!camp.var_mapping) camp.var_mapping={};
        ['subject_1','body_1','subject_2','body_2','subject_3','body_3','subject_4','body_4','subject_5','body_5'].forEach(function(ourVar){
          if(!camp.var_mapping[ourVar]){
            var match=camp.custom_vars.find(function(sv){ return sv.toLowerCase()===ourVar.toLowerCase(); });
            if(match){ camp.var_mapping[ourVar]=match; log('Campaigns: auto-mapped '+ourVar+' → '+match,'gray'); }
          }
        });

        camp.dmand_leads=(DB.contacts||[]).filter(function(c){return String(c.smartlead_campaign_id)===String(camp.id);}).length;
        log('Campaigns: "'+camp.name+'" — '+camp.sequences.length+' steps, vars: ['+camp.custom_vars.join(', ')+'], mapped: '+Object.keys(camp.var_mapping).length,'gray');
      }catch(e){
        log('Campaigns: detail fetch error for "'+camp.name+'" ('+camp.id+'): '+e.message,'red');
      }
    }));

    save(); renderCampaignList();
    document.getElementById('nb-campaigns').textContent=DB.campaigns.length+(DB.hrCampaigns?DB.hrCampaigns.length:0);
    showAlert('Loaded '+list.length+' campaign(s) with sequences','success');
  }catch(e){
    log('Refresh error: '+e.message,'red');
    showAlert('Error: '+e.message,'error');
  }
}

function toggleCampCard(id){
  var camp=DB.campaigns.find(function(c){return String(c.id)===String(id);});
  if(!camp) return;
  camp._expanded=!camp._expanded;
  var body=document.getElementById('camp-body-'+id);
  if(body) body.style.display=camp._expanded?'block':'none';
  var header=body&&body.previousElementSibling;
  if(header){
    var arrow=header.querySelector('span:last-child');
    if(arrow) arrow.textContent=camp._expanded?'▲':'▼';
  }
}

function setCampaignMode(id, mode){
  var camp=DB.campaigns.find(function(c){return String(c.id)===String(id);});
  if(camp){ camp.mode=mode; save(); renderContacts(false); }
}

function renderCampaignList(){
  var el=document.getElementById('campaign-list');
  if(!el) return;
  if(!DB.campaigns||!DB.campaigns.length) DB.campaigns=[];
  if(!DB.hrCampaigns||!DB.hrCampaigns.length) DB.hrCampaigns=[];
  var totalCount=DB.campaigns.length+DB.hrCampaigns.length;
  if(!totalCount){
    el.innerHTML='<div class="empty"><div class="empty-icon">◎</div><div class="empty-title">No campaigns synced</div><div class="empty-sub">Create your campaigns in Smartlead or HeyReach, then sync above</div></div>';
    document.getElementById('nb-campaigns').textContent=0;
    return;
  }
  var hideUnassigned=document.getElementById('hide-unassigned');
  var doHide=hideUnassigned?hideUnassigned.checked:true;
  var html='';

  if(campFilter==='all'||campFilter==='smartlead'){
    var slCamps=DB.campaigns.filter(function(camp){return !(doHide&&!camp.mode);});
    if(campFilter==='all'&&slCamps.length){
      html+='<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);margin-bottom:8px;padding:6px 0;border-bottom:1px solid var(--border)">📧 Smartlead Email Campaigns</div>';
    }
    slCamps.forEach(function(camp){
      var sc=camp.status==='ACTIVE'?'var(--green)':camp.status==='PAUSED'?'var(--amber)':'var(--text3)';
      var seqs=camp.sequences||[];
      var dmandLeads=DB.contacts.filter(function(c){return String(c.smartlead_campaign_id)===String(camp.id);}).length;
      var mapping=camp.var_mapping||{};
      var cid=String(camp.id);
      var isExpanded=camp._expanded||false;
      var seqSummary='';
      if(seqs.length){
        seqSummary='<div style="margin-top:8px;display:flex;flex-direction:column;gap:3px">';
        seqs.forEach(function(seq,i){
          var v=(seq.variants&&seq.variants[0])||{};
          var subj=seq.subject||(v.subject)||'';
          var delay=seq.seq_delay_details&&seq.seq_delay_details.delay_in_days!=null?seq.seq_delay_details.delay_in_days:0;
          var isNewThread=!!(subj&&subj.trim());
          seqSummary+='<div style="display:flex;align-items:center;gap:8px;font-size:10px;font-family:var(--mono);color:var(--text3)">'
            +'<span style="color:var(--accent);font-weight:700">Step '+(i+1)+'</span>'
            +'<span>Day '+delay+'</span>'
            +'<span style="padding:1px 5px;border-radius:3px;background:'+(isNewThread?'rgba(99,102,241,0.1)':'rgba(245,158,11,0.1)')+';color:'+(isNewThread?'var(--blue)':'var(--amber)')+';">'+(isNewThread?'✉ New thread':'↩ Follow-up')+'</span>'
            +(subj?'<span style="color:var(--text2)">'+esc(subj.slice(0,50))+'</span>':'')
          +'</div>';
        });
        seqSummary+='</div>';
      }
      var slVars=camp.custom_vars||[];
      var ourVars=[];
      var numStepsForMap=Math.min(seqs.length||3,5);
      for(var vi=1;vi<=numStepsForMap;vi++){
        var seq=seqs[vi-1]||{};
        var seqSubject=seq.subject||(seq.variants&&seq.variants[0]&&seq.variants[0].subject)||'';
        var isNew=!!(seqSubject&&seqSubject.trim());
        ourVars.push({key:'subject_'+vi,label:'Subject_'+vi,desc:'Step '+vi+' subject'+(isNew?'':' (follow-up)')});
        ourVars.push({key:'body_'+vi,label:'Body_'+vi,desc:'Step '+vi+' email body'});
      }
      if(slVars.length){
        ourVars.forEach(function(ov){
          if(!mapping[ov.key]){
            var match=slVars.find(function(sv){ return sv.toLowerCase()===ov.key.toLowerCase()||sv.toLowerCase()===ov.label.toLowerCase(); });
            if(match){ mapping[ov.key]=match; camp.var_mapping=mapping; save(); }
          }
        });
      }
      var mappingRows=ourVars.map(function(ov){
        var mapped=mapping[ov.key]||'';
        var isMapped=!!(mapped&&(slVars.length===0||slVars.indexOf(mapped)>=0));
        var opts='<option value="">'+(slVars.length?'— not mapped —':'type variable name...')+'</option>'+slVars.map(function(v){return '<option value="'+esc(v)+'"'+(v===mapped?' selected':'')+'>{{'+esc(v)+'}}</option>';}).join('');
        return '<div style="display:flex;align-items:center;gap:6px;padding:4px 8px;background:var(--surface);border:1px solid '+(isMapped?'rgba(22,163,74,0.3)':'var(--border)')+';border-radius:4px">'
          +'<span style="font-size:10px;font-family:var(--mono);color:'+(isMapped?'var(--green)':'var(--text2)')+';min-width:80px;flex-shrink:0">'+ov.label+'</span>'
          +'<span style="font-size:9px;color:var(--text3);flex:1">'+ov.desc+'</span>'
          +'<span style="font-size:9px;color:var(--text3)">→</span>'
          +(slVars.length
            ?'<select data-cid="'+esc(cid)+'" data-ov="'+esc(ov.key)+'" onchange="saveCampVarMapping(this.dataset.cid,this.dataset.ov,this.value)" style="font-size:10px;font-family:var(--mono);padding:2px 6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:3px">'+opts+'</select>'
            :'<input type="text" value="'+esc(mapped)+'" placeholder="e.g. body_1" data-cid="'+esc(cid)+'" data-ov="'+esc(ov.key)+'" onchange="saveCampVarMapping(this.dataset.cid,this.dataset.ov,this.value)" style="font-size:10px;font-family:var(--mono);padding:2px 6px;background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:3px;width:120px">'
          )
          +'</div>';
      }).join('');
      var exHasContent=camp.example_steps&&camp.example_steps.some(function(s){return s&&(s.body||s.cta);});
      var exPreview='';
      if(exHasContent){
        exPreview='<div style="display:flex;flex-direction:column;gap:3px;margin-bottom:6px">';
        (camp.example_steps||[]).forEach(function(s,i){
          if(!s||(!s.body&&!s.cta)) return;
          exPreview+='<div style="background:rgba(217,119,87,0.05);border:1px solid rgba(217,119,87,0.15);border-radius:5px;padding:6px 10px">'
            +'<div style="font-size:9px;font-family:var(--mono);color:var(--accent);margin-bottom:2px;font-weight:700">Step '+(i+1)+'</div>'
            +(s.subject?'<div style="font-size:10px;color:var(--text3);font-style:italic">Subj: '+esc(s.subject.slice(0,60))+'</div>':'')
            +(s.body?'<div style="font-size:10px;color:var(--text3)">'+esc(s.body.slice(0,80))+'…</div>':'')
          +'</div>';
        });
        exPreview+='</div>';
      }
      var numSteps=seqs.length||3;
      var exFields='';
      for(var si=0;si<numSteps;si++){
        var saved=(camp.example_steps&&camp.example_steps[si])||{};
        var stepSeq=seqs[si]||{};
        var stepSubjRaw=stepSeq.subject||(stepSeq.variants&&stepSeq.variants[0]&&stepSeq.variants[0].subject)||'';
        var stepHasSubj=!!(stepSubjRaw&&stepSubjRaw.trim());
        exFields+='<div style="margin-bottom:10px;padding:10px;background:var(--surface2);border-radius:6px;border:1px solid var(--border)">'
          +'<div style="font-size:10px;font-family:var(--mono);font-weight:700;color:var(--accent);margin-bottom:8px">Step '+(si+1)+(stepHasSubj?' <span style="color:var(--blue);font-weight:400">✉ New thread</span>':' <span style="color:var(--amber);font-weight:400">↩ Follow-up</span>')+'</div>'
          +(stepHasSubj?'<div style="margin-bottom:6px"><label style="font-size:11px;color:var(--text2);display:block;margin-bottom:3px">Subject example</label><input id="ex-subject-'+cid+'-'+si+'" class="form-input" style="font-size:12px" value="'+esc(saved.subject||'')+'" placeholder="e.g. Congrats on the new role, {{first_name}}"></div>':'')
          +'<div><label style="font-size:11px;color:var(--text2);display:block;margin-bottom:3px">Body example <span style="font-size:10px;color:var(--text3)">(full email — tone guide for GPT)</span></label>'
          +'<textarea id="ex-body-'+cid+'-'+si+'" class="form-input" rows="4" style="font-size:12px;resize:vertical" placeholder="Write your full step '+(si+1)+' email...">'+esc(saved.body||'')+'</textarea></div>'
          +'</div>';
      }
      html+='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden">'
        +'<div data-campid="'+camp.id+'" onclick="toggleCampCard(this.dataset.campid)" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 16px;cursor:pointer;user-select:none">'
          +'<div style="width:8px;height:8px;border-radius:50%;background:'+sc+';flex-shrink:0"></div>'
          +'<div style="font-size:13px;font-weight:700;flex:1;min-width:0">'+esc(camp.name)+'</div>'
          +'<span style="font-size:9px;font-family:var(--mono);padding:2px 7px;border-radius:3px;background:'+sc+'22;color:'+sc+'">'+esc(camp.status||'UNKNOWN')+'</span>'
          +'<span style="font-size:9px;font-family:var(--mono);color:var(--text3)">'+seqs.length+' steps</span>'
          +'<span style="font-size:9px;font-family:var(--mono);color:var(--text3)">SL: '+(camp.lead_count||0)+' · Dmand: '+dmandLeads+'</span>'
          +'<select onclick="event.stopPropagation()" onchange="setCampaignMode('+camp.id+',this.value)" style="font-size:11px;font-family:var(--mono);padding:3px 7px;background:var(--surface);border:1px solid var(--border);color:var(--text);border-radius:4px;cursor:pointer">'
            +'<option value="" '+(camp.mode===''?'selected':'')+'>— Unassigned</option>'
            +'<option value="signal" '+(camp.mode==='signal'?'selected':'')+'>📡 Signal</option>'
            +'<option value="icebreaker" '+(camp.mode==='icebreaker'?'selected':'')+'>🧊 Signal+IB</option>'

          +'</select>'
          +'<button onclick="event.stopPropagation();removeCampaignLocal('+camp.id+')" class="btn btn-sm btn-ghost">Remove</button>'
          +'<span style="font-size:12px;color:var(--text3);margin-left:4px">'+(isExpanded?'▲':'▼')+'</span>'
        +'</div>'
        +'<div id="camp-body-'+camp.id+'" style="display:'+(isExpanded?'block':'none')+';padding:0 16px 14px 16px;border-top:1px solid var(--border)">'
          +(seqs.length?'<div style="margin-top:10px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);margin-bottom:4px">Sequence ('+seqs.length+' steps · plain text)</div>':'')
          +seqSummary
          +'<div style="margin-top:12px;border-top:1px solid var(--border);padding-top:10px">'
            +'<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3);margin-bottom:6px">Variable mapping — Dmand vars → Smartlead custom vars'+(slVars.length?'':' <span style="color:var(--amber)">(Sync to auto-detect vars)</span>')+'</div>'
            +'<div style="display:flex;flex-direction:column;gap:4px">'+mappingRows+'</div>'
          +'</div>'
          +'<div style="margin-top:12px;border-top:1px solid var(--border);padding-top:10px">'
            +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">'
              +'<div id="camp-ex-hdr-'+cid+'" style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3)">'+(exHasContent?'✓ Tone examples saved':'Tone examples for GPT')+'</div>'
              +'<button id="camp-ex-btn-'+cid+'" onclick="event.stopPropagation();toggleCampExamples(this.dataset.cid)" data-cid="'+esc(cid)+'" style="font-size:10px;font-family:var(--mono);background:none;border:none;color:var(--accent);cursor:pointer;padding:0">'+(exHasContent?'Edit examples':'+ Add examples')+'</button>'
            +'</div>'
            +'<div id="camp-ex-preview-'+cid+'">'+exPreview+'</div>'
            +'<div id="camp-examples-'+cid+'" style="display:none">'+exFields
              +'<div style="display:flex;align-items:center;gap:8px;margin-top:4px">'
                +'<button class="btn btn-accent btn-sm" onclick="saveCampExamples(this.getAttribute(\"data-cid\"),'+numSteps+')" data-cid="'+esc(cid)+'">Save examples →</button>'
                +'<span id="ex-status-'+cid+'" style="font-size:11px;font-family:var(--mono);color:var(--green)"></span>'
              +'</div>'
            +'</div>'
          +'</div>'
        +'</div>'
      +'</div>';
    });
  }

  if(campFilter==='all'||campFilter==='heyreach'){
    var hrCamps=DB.hrCampaigns.filter(function(c){return !(doHide&&(!c.hr_mode||c.hr_mode===''));});
    if(campFilter==='all'&&hrCamps.length){
      html+='<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:#0a66c2;margin:16px 0 8px;padding:6px 0;border-bottom:1px solid rgba(10,102,194,0.15)">💼 HeyReach LinkedIn Campaigns</div>';
    }
    hrCamps.forEach(function(hrc){
      html+=renderHrCampaignCard(hrc);
    });
    if(campFilter==='heyreach'&&!hrCamps.length){
      html='<div class="empty"><div class="empty-icon">◎</div><div class="empty-title">No HeyReach campaigns</div><div class="empty-sub">Click "💼 Sync HeyReach" to load your campaigns</div></div>';
    }
  }

  el.innerHTML=html||'<div class="empty"><div class="empty-icon">◎</div><div class="empty-title">No campaigns match filter</div><div class="empty-sub">Try changing the filter above or sync your campaigns</div></div>';
  document.getElementById('nb-campaigns').textContent=(DB.campaigns.length+DB.hrCampaigns.length);
}

function removeCampaignLocal(id){
  DB.campaigns=DB.campaigns.filter(function(c){return String(c.id)!==String(id);});
  save(); renderCampaignList();
}

function saveCampExamples(campId, numSteps){
  var camp=DB.campaigns.find(function(c){return String(c.id)===String(campId);});
  if(!camp) return;
  numSteps=numSteps||1;
  var steps=[];
  for(var i=0;i<numSteps;i++){
    var bodyEl=document.getElementById('ex-body-'+campId+'-'+i);
    var subjEl=document.getElementById('ex-subject-'+campId+'-'+i);
    steps.push({
      subject:(subjEl?subjEl.value:'').trim(),
      body:(bodyEl?bodyEl.value:'').trim()
    });
  }
  camp.example_steps=steps;
  save(); // save without re-rendering — would collapse the card
  var st=document.getElementById('ex-status-'+campId);
  if(st){st.textContent='✓ Saved';setTimeout(function(){st.textContent='';},2000);}
  var previewEl=document.getElementById('camp-ex-preview-'+campId);
  if(previewEl){
    var hasContent=steps.some(function(s){return s&&(s.body||s.cta);});
    previewEl.innerHTML=hasContent?steps.map(function(s,i){
      if(!s||(!s.body&&!s.cta)) return '';
      return '<div style="background:rgba(217,119,87,0.05);border:1px solid rgba(217,119,87,0.15);border-radius:5px;padding:6px 10px;margin-bottom:4px">'
        +'<div style="font-size:9px;font-family:var(--mono);color:var(--accent);margin-bottom:2px;font-weight:700">Step '+(i+1)+'</div>'
        +(s.subject?'<div style="font-size:10px;color:var(--text3);font-style:italic">Subj: '+s.subject.slice(0,60)+'</div>':'')
        +(s.body?'<div style="font-size:10px;color:var(--text3)">'+s.body.slice(0,80)+'…</div>':'')
        +'</div>';
    }).filter(Boolean).join(''):'';
    var hdrEl=document.getElementById('camp-ex-hdr-'+campId);
    if(hdrEl) hdrEl.textContent=hasContent?'✓ Tone examples saved':'Tone examples for GPT';
  }
  log('Example emails saved ('+numSteps+' steps) for "'+camp.name+'"','green');
}

function toggleCampExamples(campId){
  var body=document.getElementById('camp-body-'+campId);
  if(body&&body.style.display==='none'){
    var camp=DB.campaigns&&DB.campaigns.find(function(c){return String(c.id)===String(campId);});
    if(camp){ camp._expanded=true; body.style.display='block'; }
  }
  var el=document.getElementById('camp-examples-'+campId);
  if(!el) return;
  var isHidden=el.style.display==='none'||el.style.display==='';
  el.style.display=isHidden?'block':'none';
  var btn=document.getElementById('camp-ex-btn-'+campId);
  if(btn) btn.textContent=isHidden?'- Hide examples':'Edit examples';
}

function saveCampVarMapping(campId, ourVar, slVar){
  var camp=DB.campaigns.find(function(c){return String(c.id)===String(campId);});
  if(!camp) return;
  if(!camp.var_mapping) camp.var_mapping={};
  if(slVar) camp.var_mapping[ourVar]=slVar;
  else delete camp.var_mapping[ourVar];
  save();
  log('Mapped '+ourVar+' → '+(slVar||'(unmapped)')+' for "'+camp.name+'"','green');
}

function loadCampFormDefaults(){}

function openEmailModalWithCampaign(contactId, campaignId){
  if(!campaignId) return;
  if(campaignId==='__goto__'){showPage('campaigns',document.querySelector('[onclick*="campaigns"]'));return;}
  var camp=DB.campaigns.find(function(c){return String(c.id)===String(campaignId);});
  if(!camp){showAlert('Campaign not found. Refresh campaigns first.','error');return;}
  if(!DB.outreach||!DB.outreach.smartleadKey){showAlert('Add Smartlead API key in Keys tab.','error');return;}
  if(!DB.settings.oaiKey){showAlert('Add OpenAI API key in Keys tab.','error');return;}

  emailModalContactId=contactId;
  emailModalContactIds=[contactId];
  emailModalType=camp.mode||'signal';
  emailModalCampaignId=camp.id;
  emailModalCampaignName=camp.name;
  generatedSteps=[];

  var c=DB.contacts.find(function(x){return x.id===contactId;});
  var seqs=camp.sequences||[];
  var stepCount=seqs.length||3;

  document.getElementById('email-modal-title').textContent=esc(c?c.name||'Contact':'Contact');
  document.getElementById('email-modal-sub').textContent=(c?esc(c.title||''):'')+(c&&c.company?' · '+esc(c.company):'');

  var modeLabels={signal:'📡 Signal-based',icebreaker:'🧊 Signal + Icebreaker','':`— Unassigned`};
  var modeColors={signal:'var(--accent)',icebreaker:'var(--purple)','':`var(--text3)`};
  document.getElementById('modal-camp-name').textContent=camp.name;
  var badge=document.getElementById('modal-camp-mode-badge');
  badge.textContent=modeLabels[camp.mode||'']||camp.mode;
  badge.style.color=modeColors[camp.mode||'']||'var(--text3)';
  badge.style.background=(modeColors[camp.mode||'']||'var(--text3)').replace(')',',0.1)').replace('var(','rgba(').replace('--accent','200,240,96').replace('--purple','176,96,240').replace('--blue','52,152,219').replace('--text3','150,150,150');
  document.getElementById('modal-camp-steps-badge').textContent=stepCount+' step'+(stepCount!==1?'s':'')+' · '+stepCount*3+'-day sequence';

  var genBtn=document.getElementById('btn-generate-emails');
  if(genBtn) genBtn.textContent='✨ Generate '+stepCount+'-step sequence with GPT';

  var sendDesc=document.getElementById('email-send-desc');
  if(sendDesc) sendDesc.textContent='Push to "'+camp.name+'" · '+stepCount+' emails · '+stepCount*3+' days total';

  var stepsContainer=document.getElementById('email-steps-container');
  var sendRow=document.getElementById('email-send-row');
  document.getElementById('email-send-status').textContent='';

  stepsContainer.innerHTML='<div class="empty" style="margin:24px 0"><div class="empty-icon" style="font-size:24px">✨</div><div class="empty-title" style="font-size:13px">Click Generate to write '+stepCount+' personalised emails</div><div class="empty-sub" style="font-size:11px">Mode: '+esc(modeLabels[camp.mode||'']||camp.mode)+'</div></div>';
  sendRow.style.display='none';

  document.getElementById('email-modal-overlay').style.display='block';
  document.getElementById('email-modal').style.display='block';

  setTimeout(function(){
    var genBtn=document.getElementById('btn-generate-emails');
    if(genBtn&&!genBtn.disabled) genBtn.click();
  },200);
}

async function syncReplies(){
  var key=DB.outreach&&DB.outreach.smartleadKey;
  if(!key){showAlert('Add Smartlead API key first.','error');return;}
  if(!DB.campaigns||!DB.campaigns.length){showAlert('No campaigns. Refresh campaigns first.','warning');return;}

  var synced=0;
  for(var i=0;i<DB.campaigns.length;i++){
    var camp=DB.campaigns[i];
    try{
      var r=await fetch(PROXY+'/smartlead/api/v1/campaigns/'+camp.id+'/leads?api_key='+encodeURIComponent(key));
      var data=await r.json();
      var leads=data.data||data.leads||[];
      log('Sync '+camp.name+': '+leads.length+' leads','blue');
      leads.forEach(function(lead){
        var email=(lead.email||'').toLowerCase();
        var contact=DB.contacts.find(function(c){
          return (c.business_email||'').toLowerCase()===email
            ;
        });
        if(contact){
          var ls=(lead.lead_status||lead.status||'').toUpperCase();
          if(ls==='REPLIED') contact.sl_reply_status='replied';
          else if(ls==='BOUNCED'||ls==='BOUNCE') contact.sl_reply_status='bounced';
          else if(ls==='COMPLETED') contact.sl_reply_status='completed';
          else if(ls==='INPROGRESS'||ls==='STARTED') contact.sl_reply_status='sent';
          contact.smartlead_campaign_id=camp.id;
          synced++;
        }
      });
      camp.lead_count=leads.length;
    }catch(e){
      log('Sync error '+camp.name+': '+e.message,'red');
    }
  }
  save(); renderContacts(false); renderCampaignList();
  showAlert('Synced '+synced+' contact statuses from Smartlead','success');
  log('TIP: LinkedIn status updates come via HeyReach webhooks or manual sync','gray');
}

function saveFullenrichKey(){
  const val = document.getElementById('fullenrich-key').value.trim();
  if(!val){ showAlert('Enter your FullEnrich API key.','error'); return; }
  DB.settings.fullenrichKey = val;
  save();
  document.getElementById('fullenrich-status').textContent = 'Saved \u2713';
  showAlert('FullEnrich key saved. Select contacts and click "Enrich emails".','success');
}
function loadFullenrichKeyUI(){
  const el = document.getElementById('fullenrich-key');
  const st = document.getElementById('fullenrich-status');
  if(el && DB.settings.fullenrichKey){ el.value = DB.settings.fullenrichKey; if(st) st.textContent = 'Active \u2713'; }
}

async function enrichSelectedEmails(){
  const feKey = DB.settings.fullenrichKey;
  if(!feKey){ showAlert('Add your FullEnrich API key in the Keys tab.','error'); return; }

  const selected = [...selectedContactIds]
    .map(id => DB.contacts.find(c => c.id === id))
    .filter(c => c && c.linkedin);

  const skipped = selectedContactIds.size - selected.length;
  if(!selected.length){ showAlert('No selected contacts have a LinkedIn URL — cannot enrich.','warning'); return; }

  const msg = 'Enrich ' + selected.length + ' contact(s) for email?\n'
    + 'Estimated cost: ~' + selected.length + ' FullEnrich credit(s).'
    + (skipped > 0 ? '\n(' + skipped + ' contact(s) skipped — no LinkedIn URL)' : '');
  if(!confirm(msg)) return;

  showAlert('Enriching ' + selected.length + ' contacts via FullEnrich…','info', 0);
  log('FullEnrich: starting email enrichment for ' + selected.length + ' contacts', 'green');

  selected.forEach(c => { c.email_status = 'enriching'; });
  save(); renderContacts(false);

  let enriched = 0, failed = 0;

  for(let i = 0; i < selected.length; i++){
    const c = selected[i];
    const nameParts = (c.name||'').trim().split(/\s+/);
    const firstname = nameParts[0] || '';
    const lastname  = nameParts.slice(1).join(' ') || '';

    try {
      const startRes = await fetch(PROXY + '/fullenrich/api/v1/contact/enrich/bulk', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + feKey,
          'Content-Type':  'application/json',
          'Accept':        'application/json'
        },
        body: JSON.stringify({
          name: c.name,
          datas: [{
            firstname:    firstname,
            lastname:     lastname,
            linkedin_url: c.linkedin,
            domain:       c.domain || '',
            company_name: c.company || '',
            enrich_fields: ['contact.emails'],
            custom: { contact_id: c.id }
          }]
        })
      });

      const startText = await startRes.text();
      log('FullEnrich start [' + c.name + '] HTTP ' + startRes.status + ' — ' + startText.slice(0,100), 'blue');

      if(!startRes.ok){
        c.email_status = 'error';
        failed++;
        log('FullEnrich start error: ' + startText.slice(0,200), 'red');
        continue;
      }

      const startData = JSON.parse(startText);
      const enrichmentId = startData.id || startData.enrichment_id;
      if(!enrichmentId){
        c.email_status = 'error';
        failed++;
        log('FullEnrich: no enrichment_id in response', 'red');
        continue;
      }

      log('FullEnrich: enrichment started id=' + enrichmentId + ' for ' + c.name, 'blue');

      let result = null;
      for(let attempt = 0; attempt < 20; attempt++){
        await new Promise(r => setTimeout(r, 3000));
        const pollRes = await fetch(PROXY + '/fullenrich/api/v1/contact/enrich/bulk/' + enrichmentId, {
          headers: { 'Authorization': 'Bearer ' + feKey, 'Accept': 'application/json' }
        });
        const pollText = await pollRes.text();
        const pollData = JSON.parse(pollText);
        const pollStatus = (pollData.status||'').toUpperCase();
        log('FullEnrich poll [' + c.name + '] attempt ' + (attempt+1) + ' status=' + pollStatus, 'blue');

        if(pollStatus === 'DONE' || pollStatus === 'COMPLETED' || pollStatus === 'FINISHED'){
          result = pollData;
          break;
        }
        if(pollStatus === 'ERROR' || pollStatus === 'FAILED' || pollStatus === 'CANCELED'){
          log('FullEnrich: enrichment ' + pollStatus, 'red');
          break;
        }
      }

      if(!result){
        c.email_status = 'timeout';
        failed++;
        log('FullEnrich: timeout for ' + c.name, 'amber');
        continue;
      }

      const contactResult = (result.datas || [])[0] || {};
      const contactData   = contactResult.contact || {};
      const emails        = contactData.emails || [];
      const mostProbable  = contactData.most_probable_email || '';

      log('FullEnrich result for ' + c.name + ': most_probable=' + mostProbable + ' emails=' + JSON.stringify(emails).slice(0,200), 'green');

      const PERSONAL_DOMAINS = ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com',
        'proton.me','protonmail.com','me.com','aol.com','live.com','msn.com','ymail.com',
        'googlemail.com','mail.com','zoho.com','fastmail.com','hey.com','tutanota.com'];

      const allEmails = emails.map(function(e){ return e.email; }).filter(Boolean);

      const bizEmail = mostProbable || allEmails[0] || '';

      const perEmail = allEmails.find(function(e){
        if(e === bizEmail) return false;
        const domain = (e.split('@')[1]||'').toLowerCase();
        return PERSONAL_DOMAINS.indexOf(domain) >= 0;
      }) || '';

      c.business_email = bizEmail || 'N/A';
      c.email_status   = 'done';
      c.email_enriched_at = new Date().toISOString();
      enriched++;
      log('── FullEnrich ✓ '+c.name+' | biz: '+c.business_email+' | personal: '+(c.personal_email||'none'),'green');

      triggerInsightsForContact(c); // fire-and-forget, non-blocking

    } catch(e) {
      c.email_status = 'error';
      failed++;
      log('FullEnrich error [' + c.name + ']: ' + e.message, 'red');
    }

    save();
    renderContacts(false);
  }

  selectedContactIds.clear();
  updateSelectionUI();
  save();
  renderContacts(false);

  const summary = 'Email enrichment done: ' + enriched + ' enriched'
    + (failed > 0 ? ', ' + failed + ' failed' : '')
    + (skipped > 0 ? ', ' + skipped + ' skipped (no LinkedIn)' : '');
  showAlert(summary, enriched > 0 ? 'success' : 'warning');
  log(summary, enriched > 0 ? 'green' : 'amber');
}

function saveCrustdataKey(){
  const val=document.getElementById('crustdata-key').value.trim();
  if(!val){showAlert('Enter your Crustdata API key.','error');return;}
  DB.settings.crustdataKey=val;
  save();
  document.getElementById('crustdata-status').textContent='Saved ✓';
  showAlert('Crustdata key saved. Super Enrich is now active on enriched events.','success');
}
function loadCrustdataKeyUI(){
  if(DB.settings.crustdataKey){
    document.getElementById('crustdata-key').value=DB.settings.crustdataKey;
    document.getElementById('crustdata-status').textContent='Active ✓';
  }
}

async function superEnrichEvent(eventId){
  const ev=DB.events.find(e=>e.event_id===eventId);
  if(!ev){showAlert('Event not found.','error');return;}

  const domain = ev.company_domain || ev.enrichment?.data?.domain || '';
  const linkedinUrl = ev.company_linkedin_url || '';

  if(!domain && !linkedinUrl){
    showAlert('No domain or LinkedIn URL available — cannot Super Enrich.','warning');
    return;
  }

  const key=DB.settings.crustdataKey;
  if(!key){showAlert('Add your Crustdata API key in the Keys tab.','error');return;}

  const btn=document.getElementById('super-enrich-btn-'+eventId);
  if(btn){btn.textContent='Enriching…';btn.disabled=true;}

  ev.superEnrichment={status:'loading'};
  log('Crustdata super enrich: '+(domain||linkedinUrl),'blue');

  async function crustCompanyFetch(identifier, label){
    const res=await fetch(PROXY+'/crustdata/company/enrich',{
      method:'POST',
      headers:{
        'Authorization':'Bearer '+key,
        'Content-Type':'application/json',
        'Accept':'application/json',
        'x-api-version':'2025-11-01'
      },
      body:JSON.stringify(identifier),
      signal:AbortSignal.timeout(20000)
    });
    const txt=await res.text();
    log('Crustdata ['+label+']: HTTP '+res.status+' — '+txt.slice(0,80),'blue');
    if(res.status===402){
      log('Crustdata: ✗ Out of credits (402) — top up at crustdata.com','red');
      showAlert('Crustdata out of credits — please top up your account','error',0);
      ev.superEnrichment={status:'credit_error',message:'Out of Crustdata credits (402)'};
      save(); renderEvents();
      throw new Error('CRUSTDATA_402');
    }
    if(!res.ok) return null;
    const arr=JSON.parse(txt);
    const entry=Array.isArray(arr)?arr[0]:arr;
    const matches=(entry?.matches||[]).sort((a,b)=>(b.confidence_score||0)-(a.confidence_score||0));
    return {matches, rawText:txt};
  }

  try{
    let matchArr=[], rawText='';

    if(domain){
      const r=await crustCompanyFetch({domains:[domain],exact_match:true,fields:['basic_info','headcount','funding','locations','taxonomy','hiring']},'domain exact');
      if(r){matchArr=r.matches;rawText=r.rawText;}
    }

    if(!matchArr.length&&domain){
      log('Crustdata: no exact match — retrying without exact_match','amber');
      const r=await crustCompanyFetch({domains:[domain],fields:['basic_info','headcount','funding','locations','taxonomy','hiring']},'domain fuzzy');
      if(r){matchArr=r.matches;rawText=r.rawText;}
    }

    if(!matchArr.length&&linkedinUrl){
      log('Crustdata: domain failed — retrying with LinkedIn URL','amber');
      const r=await crustCompanyFetch({linkedin_url:linkedinUrl,fields:['basic_info','headcount','funding','locations','taxonomy','hiring']},'linkedin_url');
      if(r){matchArr=r.matches;rawText=r.rawText;}
    }

    const match = matchArr[0];
    if(!match||!match.company_data){
      log('Crustdata super enrich: ✗ no match for domain="'+domain+'" linkedin="'+linkedinUrl+'" — all strategies exhausted','red');
      ev.superEnrichment={status:'error',message:'No company data found for '+(domain||linkedinUrl)+'. Raw: '+rawText.slice(0,200),exhausted:true};
      save(); renderEvents();
      return;
    }
    log('Crustdata super enrich: ✓ matched via '+( match.match_type||'unknown')+' confidence='+( match.confidence_score||'?'),'green');

    const cd = match.company_data;
    log('Crustdata company_data sections: '+JSON.stringify(Object.keys(cd)).slice(0,200),'blue');

    const bi  = cd.basic_info||{};
    const hc  = cd.headcount||{};
    const fu  = cd.funding||{};
    const lo  = cd.locations||{};
    const tx  = cd.taxonomy||{};
    const hi  = cd.hiring||{};

    log('basic_info keys: '+JSON.stringify(Object.keys(bi)),'gray');
    log('basic_info sample: '+JSON.stringify(bi).slice(0,400),'gray');

    const logoUrl = bi.logo_url||bi.logo||bi.company_logo_url||bi.profile_image_url
      ||cd.logo_url||cd.logo||cd.profile_pic_url||'';

    const liImageUrl = bi.linkedin_profile_pic_url||bi.linkedin_image_url||bi.profile_pic_url
      ||bi.cover_image_url||cd.profile_pic_url||'';

    log('Logo URL found: '+(logoUrl||'none'),'blue');
    log('LinkedIn image URL found: '+(liImageUrl||'none'),'blue');

    const headcountVal = hc.total || hc.employee_count || bi.employee_count || bi.employee_count_range || '';

    const hcGrowth = hc.growth_percent;
    const hcGrowthYoy = hcGrowth?.yoy_percent ?? hcGrowth?.yoy ?? hc.yoy_growth_percent ?? null;

    const totalFundingRaw = fu.total_investment_usd ?? fu.total_funding_usd ?? null;
    const lastRoundAmtRaw = fu.last_round_amount_usd ?? fu.last_funding_amount_usd ?? null;

    const rawInvestors = fu.investors||[];
    const investorNames = rawInvestors.map(i=>typeof i==='string'?i:(i.name||i.investor_name||'')).filter(Boolean).slice(0,5);

    const industry = (Array.isArray(bi.industries)&&bi.industries.length)?bi.industries.join(', ')
                   : bi.industry||tx.professional_network_industry||tx.linkedin_industry||'';

    ev.superEnrichment={
      status:'done',
      enriched_at:new Date().toISOString(),
      data:{
        company_name:  bi.name||bi.company_name||'',
        domain:        bi.primary_domain||bi.website||domain,
        linkedin_url:  bi.professional_network_url||bi.linkedin_url||'',
        logo_url:      logoUrl,
        li_image_url:  liImageUrl,
        industry,
        hq:            [lo.hq_city,lo.hq_state,lo.hq_country].filter(Boolean).join(', ')||lo.headquarters||'',
        headcount:     String(headcountVal||''),
        headcount_growth_yoy: hcGrowthYoy!=null?(Number(hcGrowthYoy).toFixed(1)+'%'):'',
        total_funding: totalFundingRaw!=null?('$'+Number(totalFundingRaw).toLocaleString()):'',
        last_round:    fu.last_round_type||fu.last_funding_type||fu.funding_type||'',
        last_round_amt:lastRoundAmtRaw!=null?('$'+Number(lastRoundAmtRaw).toLocaleString()):'',
        investors:     investorNames.join(', '),
        open_jobs:     String(hi.openings_count??hi.open_jobs_count??''),
      }
    };

    if(!DB.companies) DB.companies=[];
  if(!DB.autoboundKeys) DB.autoboundKeys=[];
    const existing=DB.companies.find(c=>c.domain===domain);
    const compEntry={
      id: existing?.id||'co_'+Date.now()+'_'+Math.random().toString(36).slice(2,5),
      domain,
      event_ids: [...new Set([...(existing?.event_ids||[]),eventId])],
      signal_names:[...new Set([...(existing?.signal_names||[]),ev.signal_name||''])],
      enriched_at:new Date().toISOString(),
      ...ev.superEnrichment.data
    };
    if(existing){Object.assign(existing,compEntry);}
    else{DB.companies.push(compEntry);}

    save();
    document.getElementById('nb-companies').textContent=(DB.companies||[]).length;
    renderEvents();
    renderCompanies();
    showAlert('Super Enrich done for '+domain+'!','success');
    log('Crustdata: '+domain+' — '+compEntry.company_name+' | '+compEntry.industry,'green');

    setTimeout(function(){ autoEnrichContactsForEvent(ev); }, 1500);
  }catch(e){
    const errMsg = e.name==='TimeoutError' ? 'Request timed out (20s) — proxy may be slow or Crustdata unreachable'
      : e.name==='TypeError' && e.message.includes('fetch') ? 'Network error — check proxy is running and can reach api.crustdata.com'
      : e.message;
    ev.superEnrichment={status:'error',message:errMsg};
    save(); renderEvents();
    log('Crustdata error: '+errMsg,'red');
    showAlert('Super Enrich failed: '+errMsg,'error',8000);
  }
}

function clearCompanies(){
  if(!confirm('Clear all company enrichments?')) return;
  DB.companies=[];
  DB.events.forEach(e=>{if(e.superEnrichment) e.superEnrichment=null;});
  saveAndSync(); renderCompanies();
  document.getElementById('nb-companies').textContent='0';
}

function updateCompanySelection(){
  var cbs=document.querySelectorAll('.company-row-cb:checked');
  var count=cbs.length;
  var btn=document.getElementById('delete-selected-companies-btn');
  var ct=document.getElementById('selected-companies-count');
  if(btn) btn.style.display=count>0?'':'none';
  if(ct) ct.textContent=count;
}

function deleteSelectedCompanies(){
  var cbs=document.querySelectorAll('.company-row-cb:checked');
  var domains=Array.from(cbs).map(function(cb){return cb.dataset.domain;});
  if(!domains.length) return;
  if(!confirm('Delete '+domains.length+' selected company record(s)? This cannot be undone.')) return;
  DB.companies=DB.companies.filter(function(c){return domains.indexOf(c.domain)<0;});
  saveAndSync(); renderCompanies();
  updateCompanySelection();
  showAlert(domains.length+' company record(s) deleted.','success',3000);
}

function deleteSingleCompany(domain){
  DB.companies=DB.companies.filter(function(c){return c.domain!==domain;});
  saveAndSync(); renderCompanies();
  showAlert('Company deleted.','success',2000);
}

function exportCompaniesCSV(){
  if(!DB.companies||!DB.companies.length){showAlert('No companies to export.','info');return;}
  const rows=[['Company Name','Domain','Industry','HQ','Headcount','HC Growth YoY','Total Funding','Last Round','Last Round Amt','Investors','Open Jobs','LinkedIn URL','Signals','Enriched At']];
  DB.companies.forEach(c=>rows.push([
    c.company_name||'',c.domain||'',c.industry||'',c.hq||'',
    c.headcount||'',c.headcount_growth_yoy||'',c.total_funding||'',
    c.last_round||'',c.last_round_amt||'',c.investors||'',c.open_jobs||'',
    c.linkedin_url||'',(c.signal_names||[]).join('; '),c.enriched_at||''
  ]));
  dl(new Blob([toCSV(rows)],{type:'text/csv'}),'dmand_companies_'+today()+'.csv');
}

function renderCompanies(){
  if(typeof currentPage !== 'undefined' && currentPage !== 'companies') return;
  if(!DB.companies) DB.companies=[];
  if(!DB.autoboundKeys) DB.autoboundKeys=[];
  var nbEl=document.getElementById('nb-companies');
  if(nbEl) nbEl.textContent=DB.companies.length;
  var el=document.getElementById('company-list');
  if(!el) return;

  var sf=document.getElementById('company-sig-filter');
  if(sf){
    var cur=sf.value;
    var opts='<option value="">All signals</option>'+DB.signals.map(function(s){return '<option value="'+s.id+'"'+(cur===s.id?' selected':'')+'>'+esc(s.name)+'</option>';}).join('');
    sf.innerHTML=opts;
    sf.value=cur;
  }

  var search=(document.getElementById('company-search')||{value:''}).value.toLowerCase();
  var sigFilter=sf?sf.value:'';

  var cos=DB.companies.slice().sort(function(a,b){return new Date(b.enriched_at||0)-new Date(a.enriched_at||0);});
  if(search) cos=cos.filter(function(c){return ((c.company_name||'')+(c.domain||'')+(c.industry||'')+(c.hq||'')).toLowerCase().indexOf(search)>=0;});
  if(sigFilter){var sig=DB.signals.find(function(s){return s.id===sigFilter;});if(sig) cos=cos.filter(function(c){return (c.signal_names||[]).indexOf(sig.name)>=0;});}
  var coDays=parseInt((document.getElementById('company-date-filter')||{value:''}).value)||0;
  if(coDays>0){
    var coCutoff=new Date(Date.now()-coDays*864e5).toISOString();
    cos=cos.filter(function(c){ return (c.enriched_at||'')>=coCutoff; });
  }

  if(!cos.length){
    el.innerHTML='<div class="empty"><div class="empty-icon">&#9643;</div><div class="empty-title">No companies yet</div><div class="empty-sub">Click &quot;Super Enrich &#10022;&quot; on any enriched event card to pull Crustdata company intel.</div></div>';
    return;
  }

  function v(x){return esc(String(x==null||x===''?'&#8212;':x));}
  var fieldColors={
    'Industry':     {bg:'rgba(37,99,235,0.06)',  border:'rgba(37,99,235,0.12)',  label:'rgba(37,99,235,0.7)'},
    'HQ':           {bg:'rgba(124,58,237,0.06)', border:'rgba(124,58,237,0.12)', label:'rgba(124,58,237,0.7)'},
    'Headcount':    {bg:'rgba(22,163,74,0.06)',  border:'rgba(22,163,74,0.12)',  label:'rgba(22,163,74,0.7)'},
    'HC Growth YoY':{bg:'rgba(22,163,74,0.06)',  border:'rgba(22,163,74,0.12)',  label:'rgba(22,163,74,0.7)'},
    'Total Funding':{bg:'rgba(217,119,87,0.08)', border:'rgba(217,119,87,0.2)',  label:'rgba(217,119,87,0.8)'},
    'Last Round':   {bg:'rgba(217,119,87,0.06)', border:'rgba(217,119,87,0.12)', label:'rgba(217,119,87,0.7)'},
    'Round Amount': {bg:'rgba(217,119,87,0.08)', border:'rgba(217,119,87,0.2)',  label:'rgba(217,119,87,0.8)'},
    'Open Jobs':    {bg:'rgba(245,158,11,0.06)', border:'rgba(245,158,11,0.12)', label:'rgba(245,158,11,0.8)'},
  };
  function field(label,val){
    var fc=fieldColors[label]||{bg:'var(--surface2)',border:'var(--border)',label:'var(--text3)'};
    return '<div style="background:'+fc.bg+';border:1px solid '+fc.border+';border-radius:6px;padding:8px 10px">'
      +'<div style="font-size:9px;font-family:var(--mono);color:'+fc.label+';text-transform:uppercase;letter-spacing:0.06em;margin-bottom:3px;font-weight:600">'+label+'</div>'
      +'<div style="font-size:13px;font-weight:600;color:var(--text)">'+val+'</div>'
      +'</div>';
  }
  function accentVal(x){return x?'<span style="color:var(--accent);font-weight:600">'+esc(String(x))+'</span>':'&#8212;';}
  function growthVal(x){
    if(!x) return '&#8212;';
    var n=parseFloat(x);
    var col=isNaN(n)?'var(--text2)':n>0?'var(--accent)':n<0?'var(--red)':'var(--text2)';
    return '<span style="color:'+col+';font-weight:600">'+esc(String(x))+'</span>';
  }

  var html='';
  var coTotalPages=Math.ceil(cos.length/COMPANIES_PER_PAGE);
  if(companyPage>=coTotalPages) companyPage=Math.max(0,coTotalPages-1);
  var coStart=companyPage*COMPANIES_PER_PAGE;
  var coSlice=cos.slice(coStart,coStart+COMPANIES_PER_PAGE);
  var coPagHtml=coTotalPages>1?'<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 0;margin-top:8px">'
    +'<span style="font-size:11px;font-family:var(--mono);color:var(--text3)">Showing '+(coStart+1)+'–'+Math.min(coStart+COMPANIES_PER_PAGE,cos.length)+' of '+cos.length+' companies</span>'
    +'<div style="display:flex;align-items:center;gap:6px">'
      +'<button class="btn btn-sm" onclick="companyPage=Math.max(0,companyPage-1);renderCompanies()" '+(companyPage===0?'disabled':'')+'>← Prev</button>'
      +'<span style="font-size:11px;font-family:var(--mono);color:var(--text2)">Page '+(companyPage+1)+' / '+coTotalPages+'</span>'
      +'<button class="btn btn-sm" onclick="companyPage=Math.min('+( coTotalPages-1)+',companyPage+1);renderCompanies()" '+(companyPage>=coTotalPages-1?'disabled':'')+'>Next →</button>'
    +'</div></div>':'';

  for(var ci=0;ci<coSlice.length;ci++){
    var c=coSlice[ci];
    var peopleCount = DB.contacts.filter(function(ct){ return ct.domain === c.domain; }).length;
    var liBtn=c.linkedin_url?'<a href="'+esc(c.linkedin_url)+'" target="_blank" style="font-size:10px;font-family:var(--mono);color:var(--blue);background:var(--blue-dim);padding:2px 8px;border-radius:4px;text-decoration:none;border:1px solid rgba(96,168,240,0.2)">&#8599; LinkedIn</a>':'';
    var domain_esc=esc(c.domain||'');
    var coname_esc=esc(c.company_name||c.domain||'');
    var fpBtnLabel = peopleCount > 0 ? '&#128100; '+peopleCount+' contacts · Find More' : '&#128100; Find People';
    var fpBtn='<button onclick="openFindPeople(\''+domain_esc+'\',\''+coname_esc+'\')" style="padding:5px 12px;font-size:11px;font-family:var(--mono);border:1px solid rgba(176,96,240,0.35);background:rgba(124,58,237,0.06);color:var(--purple);border-radius:20px;cursor:pointer;transition:all 0.15s;white-space:nowrap;font-weight:600">'+fpBtnLabel+'</button>';
    var logoFallbackChar=esc((c.company_name||c.domain||'?')[0].toUpperCase());
    var logoFallbackDiv='<div style="width:44px;height:44px;border-radius:10px;background:var(--accent-dim);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:700;color:var(--accent);flex-shrink:0">'+logoFallbackChar+'</div>';
    var clearbitUrl=c.domain?'https://logo.clearbit.com/'+encodeURIComponent(c.domain):'';
    var faviconUrl=c.domain?'https://www.google.com/s2/favicons?domain='+encodeURIComponent(c.domain)+'&sz=64':'';
    var logoSrc=c.logo_url||c.li_image_url||clearbitUrl;
    var logoImg=logoSrc
      ?'<div style="width:44px;height:44px;border-radius:10px;border:1px solid var(--border);background:#fff;display:flex;align-items:center;justify-content:center;flex-shrink:0;overflow:hidden;padding:2px">'
        +'<img src="'+esc(logoSrc)+'" data-fav="'+esc(faviconUrl)+'" data-fc="'+esc(logoFallbackChar)+'" style="width:38px;height:38px;object-fit:contain;border-radius:7px" onerror="logoImgError(this)">'
        +'</div>'
      :logoFallbackDiv;
    var hdr='<div style="display:flex;align-items:center;gap:14px;margin-bottom:12px;flex-wrap:wrap">'
      +logoImg
      +'<div style="flex:1;min-width:0">'
        +'<div style="font-size:16px;font-weight:700;letter-spacing:-0.02em;color:var(--text)">'+esc(c.company_name||c.domain)+'</div>'
        +'<div style="display:flex;align-items:center;gap:8px;margin-top:4px;flex-wrap:wrap">'+liBtn+'<span style="font-size:11px;font-family:var(--mono);color:var(--text3)">'+esc(c.domain)+'</span>'+fpBtn+'</div>'
      +'</div>'
      +'</div>';
    var grid='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:8px;margin-bottom:10px">'
      +field('Industry',v(c.industry))
      +field('HQ',v(c.hq))
      +field('Headcount',v(c.headcount))
      +field('HC Growth YoY',growthVal(c.headcount_growth_yoy))
      +field('Total Funding',accentVal(c.total_funding))
      +field('Last Round',v(c.last_round))
      +field('Round Amount',accentVal(c.last_round_amt))
      +field('Open Jobs',v(c.open_jobs))
      +'</div>';
    var inv=c.investors?'<div style="font-size:11px;color:var(--text2);font-family:var(--mono);margin-bottom:8px"><span style="color:var(--text3)">Investors: </span>'+esc(c.investors)+'</div>':'';
    var sigs=(c.signal_names||[]).map(function(s){return '<span class="pill pill-green">'+esc(s)+'</span>';}).join(' ');
    var dt=c.enriched_at?istDateTime(c.enriched_at):'';
    var evs=DB.events.filter(function(e){return (c.event_ids||[]).indexOf(e.event_id)>=0;});
    var evBriefHtml='';
    var eventsWithOutput=evs.filter(function(e){return e.output&&e.type==='event';});
    if(eventsWithOutput.length){
      evBriefHtml='<div style="margin-top:10px;display:flex;flex-direction:column;gap:6px">';
      eventsWithOutput.slice(0,3).forEach(function(ev){
        evBriefHtml+='<div style="background:rgba(217,119,87,0.04);border:1px solid rgba(217,119,87,0.15);border-radius:6px;padding:8px 12px">'
          +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">'
            +'<span style="font-size:9px;font-family:var(--mono);font-weight:700;color:var(--accent)">'+esc(ev.signal_name||'')+'</span>'
            +(ev.event_date?'<span style="font-size:9px;font-family:var(--mono);color:var(--text3)">'+istDate(ev.event_date)+'</span>':'')
            +(ev.qualified==='yes'?'<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:rgba(22,163,74,0.1);color:var(--green);border:1px solid rgba(22,163,74,0.2);font-weight:600">✓ ICP</span>':'')
          +'</div>'
          +'<div style="font-size:11px;color:var(--text2);line-height:1.4">'+esc((ev.output||'').slice(0,160))+(ev.output&&ev.output.length>160?'…':'')+'</div>'
        +'</div>';
      });
      evBriefHtml+='</div>';
    }
    var evList='';
    if(evs.length>1){
      evList='<details style="margin-top:8px"><summary style="font-size:11px;color:var(--text3);font-family:var(--mono);cursor:pointer">'+evs.length+' signal events &#9660;</summary><div style="margin-top:6px;display:flex;flex-direction:column;gap:4px">'+evs.map(function(ev){return '<div style="font-size:11px;font-family:var(--mono);color:var(--text2);padding:4px 8px;background:var(--surface2);border-radius:4px">'+esc(ev.signal_name)+' &middot; '+istDate(ev.event_date)+'</div>';}).join('')+'</div></details>';
    }
    html+='<div class="card" style="margin-bottom:14px;overflow:hidden"><div style="height:3px;background:linear-gradient(90deg,var(--accent),rgba(217,119,87,0.2))"></div><div class="card-body" style="padding:16px 20px">'
      +'<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap">'
      +'<div style="display:flex;align-items:flex-start;gap:10px;flex:1;min-width:0">'
        +'<input type="checkbox" class="company-row-cb" data-domain="'+domain_esc+'" onchange="updateCompanySelection()" style="margin-top:4px;cursor:pointer;accent-color:var(--accent);flex-shrink:0">'
        +'<div style="flex:1;min-width:0">'+hdr+grid+inv+'</div>'
      +'</div>'
      +'<div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end;flex-shrink:0">'+sigs+'<span style="font-size:10px;font-family:var(--mono);color:var(--text3)">'+dt+'</span>'
        +'<button onclick="deleteSingleCompany(\''+domain_esc+'\')" style="padding:2px 8px;font-size:10px;border-radius:4px;border:1px solid rgba(239,68,68,0.3);background:rgba(239,68,68,0.05);color:var(--red);cursor:pointer">Delete</button>'
      +'</div>'
      +'</div>'
      +evBriefHtml
      +evList+'</div></div>';
  }
  el.innerHTML=html+coPagHtml;
}

function renderAll(){
  requestAnimationFrame(function(){
    renderSignals(); renderEvents(); renderKeys(); renderPersonas();
    renderContacts(); renderCompanies(); updateMetrics(); renderLog();
  });
}

function today(){ return new Date().toISOString().split('T')[0]; }
function dl(blob,name){ const u=URL.createObjectURL(blob),a=document.createElement('a'); a.href=u; a.download=name; a.click(); URL.revokeObjectURL(u); }
function toCSV(rows){ return rows.map(r=>r.map(c=>'"'+String(c||'').replace(/"/g,'""')+'"').join(',')).join('\n'); }
function exportJSON(){ dl(new Blob([JSON.stringify(DB,null,2)],{type:'application/json'}),'dmand_backup_'+today()+'.json'); }
function exportCSV(){ const rows=[['id','name','category','frequency','status','monitor_id','key_label','notes','query','created_at']]; DB.signals.forEach(s=>rows.push([s.id,s.name,s.category,s.frequency,s.status,s.monitor_id||'',s.key_label||'',s.notes||'',s.query,s.created_at||''])); dl(new Blob([toCSV(rows)],{type:'text/csv'}),'dmand_signals_'+today()+'.csv'); }
function exportEventsCSV(){
  const rows=[['event_id','type','signal_name','category','output','event_date','fetched_at','source_urls']];
  DB.events.filter(e=>e.type==='event').forEach(e=>rows.push([e.event_id,e.type,e.signal_name,e.category||'',e.output,e.event_date,e.fetched_at,(e.source_urls||[]).join(' | ')]));
  dl(new Blob([toCSV(rows)],{type:'text/csv'}),'dmand_events_'+today()+'.csv');
}
function exportContactsCSV(){
  if(!DB.contacts||!DB.contacts.length){showAlert('No contacts to export.','info');return;}
  const rows=[['Source','Signal','Event Brief','Persona','Date','Name','Title','Headline','Seniority','Department','Location','Company','Domain','Business Email','Personal Email','LinkedIn','Found','Email Campaign','Email Pushed','Reply Status','LinkedIn Campaign','LinkedIn Pushed','LinkedIn Status']];
  DB.contacts.forEach(c=>rows.push([
    c.source||'parallel', c.signal_name||'', c.event_brief||'', c.persona_name||'', c.event_date||'',
    c.name||'', c.title||'', c.headline||'', c.seniority||'', c.department||'',
    c.location||'', c.company||'', c.domain||'', c.business_email||'', c.personal_email||'', c.linkedin||'', c.found_at||'',
    c.smartlead_campaign_name||'', c.smartlead_pushed_at||'', c.sl_reply_status||'',
    c.heyreach_campaign_name||'', c.heyreach_pushed_at||'', c.linkedin_status||''
  ]));
  dl(new Blob([toCSV(rows)],{type:'text/csv'}),'dmand_contacts_'+today()+'.csv');
}
function importData(evt){
  const file=evt.target.files[0]; if(!file) return;
  const r=new FileReader();
  r.onload=e=>{ try{ const d=JSON.parse(e.target.result); if(!d.signals){showAlert('Invalid file.','error');return;} if(!confirm(`Import? Signals:${d.signals.length} Events:${(d.events||[]).length} Keys:${(d.keys||[]).length} Contacts:${(d.contacts||[]).length}`)) return; DB={...DB,...d}; if(!DB.personas) DB.personas=[]; if(!DB.contacts) DB.contacts=[];
  if(!DB.companies) DB.companies=[];
  if(!DB.autoboundKeys) DB.autoboundKeys=[]; if(!DB.heyreachKeys) DB.heyreachKeys=[]; if(!DB.hrCampaigns) DB.hrCampaigns=[]; save(); renderAll(); showAlert('Imported.','success'); }catch(err){showAlert('Parse error: '+err.message,'error');} };
  r.readAsText(file); evt.target.value='';
}

const ENRICH_SCHEMA = {
  type:'json',
  json_schema:{
    type:'object',
    properties:{
      company_name:      {type:'string', description:'Official full company name'},
      domain:            {type:'string', description:'Primary website domain e.g. stripe.com'},
      industry:          {type:'string', description:'Primary industry vertical in 3-5 words'},
      hq_address:        {type:'string', description:'Full headquarters address: City, State, Country'},
      headcount:         {type:'string', description:'Approximate employee count or range e.g. 500-1000'},
      revenue_mn:        {type:'string', description:'Annual revenue in USD millions e.g. 2M. Use latest available. Write N/A if private and unknown.'},
      total_funding:     {type:'string', description:'Total funding raised to date in USD e.g. 5M Series B. Write Bootstrapped or N/A if none.'},
      last_funding_round:{type:'string', description:'Most recent funding round type and date e.g. Series B - Feb 2025'},
    },
    required:['company_name','domain','industry','hq_address','headcount','revenue_mn','total_funding','last_funding_round'],
    additionalProperties:false
  }
};

function extractCompanyName(text){
  const patterns=[
    /Becker[^:]*:\s*([A-Z][A-Za-z0-9\s&\-\.]{2,40}?)\s+(?:plans|announced|said|will|has)/,
    /^([A-Z][A-Za-z0-9\s&\-\.]{2,40}?)\s+(?:announced|raised|plans|said|has|will|signed|closed|launched)/,
    /^([A-Z][A-Za-z0-9\s&\-\.]{2,40}?)\s+(?:Inc|LLC|Corp|Ltd|Group|Health|Care|Medical|Systems)/i,
  ];
  for(const p of patterns){ const m=text.match(p); if(m&&m[1]&&m[1].trim().length>2) return m[1].trim(); }
  return text.split(' ').slice(0,4).join(' ');
}

async function enrichEvent(eventId){
  const ev=DB.events.find(e=>e.event_id===eventId);
  if(!ev) return;

  const oaiKey=DB.settings.oaiKey;
  const qualifierPrompt=DB.settings.qualifierPrompt;

  if(!oaiKey){
    log('enrichEvent: no OpenAI key — falling back to Parallel task (slow)','amber');
    await enrichEventLegacy(eventId);
    return;
  }

  ev.enrichment={status:'loading'};
  save(); renderEnrichPanel(eventId);
  log('','blue');
  log('══ PIPELINE: "'+ev.signal_name+'" | '+new Date().toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',second:'2-digit'}),'blue');

  try{
    const systemPrompt=`You are a B2B signal analyst. Given a news event, extract company info and determine ICP fit.
Return ONLY valid JSON, no markdown, no explanation:
{
  "company_name": "...",
  "company_domain": "domain.com (no https, no path)",
  "company_linkedin_url": "https://linkedin.com/company/... or empty string",
  "qualified": true or false,
  "reason": "one sentence why qualified or not"
}`;

    const userPrompt='Signal event:\n'+ev.output
      +(qualifierPrompt ? '\n\nICP qualifier criteria (answer qualified=true only if YES):\n'+qualifierPrompt : '\n\nqualified=true if this is a real company signal with clear business context.');

    const r=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+oaiKey},
      body:JSON.stringify({
        model:DB.settings.oaiModel||'gpt-4o-mini',
        max_tokens:200,
        temperature:0,
        messages:[{role:'system',content:systemPrompt},{role:'user',content:userPrompt}]
      }),
      signal:AbortSignal.timeout(20000)
    });

    const data=await r.json();
    const raw=(data.choices&&data.choices[0]&&data.choices[0].message&&data.choices[0].message.content)||'';
    const clean=raw.replace(/```json|```/g,'').trim();
    const parsed=JSON.parse(clean);

    const domain=(parsed.company_domain||'').replace(/^https?:\/\//,'').split('/')[0].trim();
    const linkedinUrl=(parsed.company_linkedin_url||'').trim();
    const companyName=(parsed.company_name||ev.company_name||'').trim();
    const isQualified=parsed.qualified===true;

    log('GPT extract+qualify ['+ev.signal_name.slice(0,20)+']: domain="'+domain+'" linkedin="'+linkedinUrl.slice(0,40)+'" name="'+companyName+'" qualified='+(isQualified?'✓ YES':'✗ NO')+(parsed.reason?' — '+parsed.reason.slice(0,80):''),'blue');

    ev.enrichment={
      status:'done',
      data:{ domain, company_name:companyName, name:companyName, linkedin_url:linkedinUrl },
      enriched_at:new Date().toISOString(),
      fast:true
    };

    if(domain) ev.company_domain=domain;
    if(linkedinUrl) ev.company_linkedin_url=linkedinUrl;
    if(companyName) ev.company_name=companyName;

    ev.qualified=isQualified?'yes':'no';

    save(); renderEnrichPanel(eventId); renderEvents();

    if(isQualified&&(!ev.superEnrichment||ev.superEnrichment.status!=='done')){
      log('ICP match — auto Super Enrich: '+ev.signal_name.slice(0,30),'green');
      setTimeout(function(){superEnrichEvent(ev.event_id);},1000);
    }

  }catch(err){
    log('GPT enrich+qualify error ['+ev.signal_name.slice(0,20)+']: '+err.message,'red');
    const fallbackDomain=ev.company_domain||'';
    if(fallbackDomain){
      ev.enrichment={status:'done',data:{domain:fallbackDomain,company_name:ev.company_name||''},enriched_at:new Date().toISOString(),fast:true};
      ev.qualified=undefined; // re-run qualifier separately
      save(); renderEnrichPanel(eventId);
      if(qualifierPrompt&&oaiKey&&ev.qualified===undefined){
        qualifyEvent(ev).then(function(isYes){
          if(isYes===null) return;
          ev.qualified=isYes?'yes':'no';
          if(isYes&&(!ev.superEnrichment||ev.superEnrichment.status!=='done')){
            setTimeout(function(){superEnrichEvent(ev.event_id);},1000);
          }
          save(); renderEvents();
        });
      }
    } else {
      ev.enrichment={status:'error',message:err.message};
      save(); renderEnrichPanel(eventId);
    }
  }
}

async function enrichEventLegacy(eventId){
  const ev=DB.events.find(e=>e.event_id===eventId);
  if(!ev) return;
  const key=getActiveKey();
  if(!key){showAlert('No active API key.','error');return;}
  if(!proxyOk){showAlert('Proxy not running.','error',0);return;}
  log('enrichEvent: ⚠ using legacy Parallel task (slow ~60s) — add OpenAI key to enable fast GPT path','amber');
  ev.enrichment={status:'loading'};
  save(); renderEnrichPanel(eventId);
  try{
    const createRes=await fetch(PROXY+'/v1/tasks/runs',{
      method:'POST',
      headers:{'x-api-key':key.value,'Content-Type':'application/json'},
      body:JSON.stringify({input:'From this news event, identify the main company:\n\n'+ev.output,processor:'core',task_spec:{output_schema:ENRICH_SCHEMA}})
    });
    if(!createRes.ok){ev.enrichment={status:'error',message:'Task failed'};save();renderEnrichPanel(eventId);return;}
    const {run_id}=await createRes.json();
    let result=null;
    for(let i=0;i<30;i++){
      await new Promise(r=>setTimeout(r,3000));
      const pollRes=await fetch(PROXY+`/v1/tasks/runs/${run_id}/result`,{headers:{'x-api-key':key.value}});
      if(!pollRes.ok) continue;
      const data=await pollRes.json();
      if(data&&data.run&&data.run.status==='completed'&&data.output){result=data.output.content;break;}
      if(data&&data.run&&data.run.status==='failed'){ev.enrichment={status:'error',message:'Task failed'};save();renderEnrichPanel(eventId);return;}
    }
    if(!result){ev.enrichment={status:'error',message:'Timed out'};save();renderEnrichPanel(eventId);return;}
    ev.enrichment={status:'done',data:result,enriched_at:new Date().toISOString()};
    save(); renderEnrichPanel(eventId);
    const qualifierPrompt=DB.settings.qualifierPrompt;
    const oaiKey=DB.settings.oaiKey;
    if(qualifierPrompt&&oaiKey&&ev.qualified===undefined){
      qualifyEvent(ev).then(function(isYes){
        if(isYes===null) return;
        ev.qualified=isYes?'yes':'no';
        if(isYes&&(!ev.superEnrichment||ev.superEnrichment.status!=='done')){
          setTimeout(function(){superEnrichEvent(ev.event_id);},1000);
        }
        save(); renderEvents();
      });
    }
  }catch(err){ev.enrichment={status:'error',message:err.message};save();renderEnrichPanel(eventId);}
}

function renderEnrichPanel(eventId){
  const panel=document.getElementById('enrich-panel-'+eventId);
  if(!panel){ renderEvents(); return; }
  const ev=DB.events.find(e=>e.event_id===eventId);
  if(!ev) return;
  const enr=ev.enrichment;

  const actionRow=panel.previousElementSibling;
  if(actionRow&&actionRow.classList.contains('enrich-action-row')){
    const hasDone=enr&&enr.status==='done';
    const hasSuperDone=ev.superEnrichment&&ev.superEnrichment.status==='done';
    actionRow.innerHTML=hasDone
      ?`<button class="enrich-btn-super ${hasSuperDone?'done':''}" id="super-enrich-btn-${eventId}" onclick="superEnrichEvent('${eventId}')">
          <svg width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.8"><polygon points="6,1 7.5,4.5 11,5 8.5,7.5 9,11 6,9.5 3,11 3.5,7.5 1,5 4.5,4.5"/></svg>
          ${hasSuperDone?'Re-Super Enrich':'Super Enrich'}
        </button>
        ${hasSuperDone?'<span class="enrich-badge" style="background:rgba(37,99,235,0.08);color:var(--blue);border-color:rgba(37,99,235,0.2)"><svg width="9" height="9" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="1,6 4,9 11,3"/></svg> Crustdata</span>':''}`
      :`<span style="font-size:11px;color:var(--text3);font-family:var(--mono);display:flex;align-items:center;gap:6px"><div class="spinner" style="width:10px;height:10px"></div>Enriching...</span>`;
  }

  if(!enr){ panel.style.display='none'; return; }
  panel.style.display='block';

  if(enr.status==='loading'){
    panel.innerHTML='<div style="display:flex;align-items:center;gap:8px;padding:12px 0;font-size:12px;color:var(--text3)"><div class="spinner"></div>Running Parallel AI enrichment task...</div>';
    return;
  }
  if(enr.status==='error'){
    panel.innerHTML='<div style="margin-top:10px;padding:10px 14px;background:var(--red-dim);border:1px solid rgba(220,38,38,0.15);border-radius:8px;font-size:12px;color:var(--red)">'+esc(enr.message)+'<br><span style="opacity:0.7;font-size:11px">Will retry on next poll.</span></div>';
    return;
  }
  if(enr.status==='done'&&enr.data){
    const d=enr.data;
    const ts=enr.enriched_at?istDateTime(enr.enriched_at):'';

    function chip(val,color){
      if(!val||val==='—') return '<span style="color:var(--text3)">—</span>';
      return '<span style="font-weight:600;color:'+color+'">'+esc(String(val))+'</span>';
    }
    function row(label,val){
      if(!val||val==='null'||val==='undefined') return '';
      return '<div style="display:flex;align-items:baseline;gap:8px;padding:5px 0;border-bottom:1px solid var(--border)">'
        +'<span style="font-size:10px;font-family:var(--mono);color:var(--text3);text-transform:uppercase;letter-spacing:0.05em;min-width:80px;flex-shrink:0">'+label+'</span>'
        +'<span style="font-size:12.5px;font-weight:500;color:var(--text)">'+val+'</span>'
        +'</div>';
    }

    var html='<div style="margin-top:12px;border-top:1px solid var(--border);padding-top:12px">';

    if(ts) html+='<div style="font-size:10px;color:var(--text3);font-family:var(--mono);margin-bottom:12px">Enriched '+ts+' via Parallel AI</div>';

    html+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px">';

    var logoHtml='';
    if(d.logo_url||d.li_image_url){
      var imgSrc=d.logo_url||d.li_image_url;
      logoHtml='<img src="'+esc(imgSrc)+'" style="width:36px;height:36px;border-radius:6px;object-fit:contain;background:#fff;border:1px solid var(--border);padding:2px;flex-shrink:0" onerror="this.style.display=\'none\'">';
    }
    html+='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:12px;border-top:3px solid var(--accent)">'
      +'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">'
        +logoHtml
        +'<div style="font-size:9px;font-family:var(--mono);font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:0.08em">Company</div>'
      +'</div>'
      +(d.company_name?row('Name','<span style="font-weight:700;color:var(--text)">'+esc(d.company_name)+'</span>'):'')
      +(d.domain?row('Domain','<a href="https://'+esc(d.domain)+'" target="_blank" style="color:var(--blue);text-decoration:none;font-family:var(--mono)">'+esc(d.domain)+'</a>'):'')
      +(d.industry?row('Industry',esc(d.industry)):'')
      +(d.hq_address?row('HQ',esc(d.hq_address)):'')
      +'</div>';

    html+='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:12px;border-top:3px solid var(--green)">'
      +'<div style="font-size:9px;font-family:var(--mono);font-weight:700;color:var(--green);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px">Size & Growth</div>'
      +(d.headcount?row('Headcount',chip(d.headcount,'var(--green)')):'')
      +(d.headcount_growth?row('HC Growth',chip(d.headcount_growth,'var(--green)')):'')
      +(d.revenue_mn?row('Revenue',esc(d.revenue_mn)):'')
      +(d.open_jobs?row('Open Jobs',esc(String(d.open_jobs))):'')
      +'</div>';

    html+='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:12px;border-top:3px solid var(--amber)">'
      +'<div style="font-size:9px;font-family:var(--mono);font-weight:700;color:var(--amber);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px">Funding</div>'
      +(d.total_funding?row('Total',chip(d.total_funding,'var(--amber)')):'')
      +(d.last_funding_round?row('Last Round',esc(d.last_funding_round)):'')
      +(d.valuation?row('Valuation',chip(d.valuation,'var(--amber)')):'')
      +(d.investors?row('Investors','<span style="font-size:11px;color:var(--text2)">'+esc(d.investors)+'</span>'):'')
      +'</div>';

    var tags=[];
    if(d.technologies&&d.technologies.length) tags=tags.concat(d.technologies.slice(0,4));
    if(d.founded) tags.push('Founded '+d.founded);
    if(tags.length){
      html+='<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:12px;border-top:3px solid var(--purple)">'
        +'<div style="font-size:9px;font-family:var(--mono);font-weight:700;color:var(--purple);text-transform:uppercase;letter-spacing:0.08em;margin-bottom:8px">Details</div>'
        +'<div style="display:flex;flex-wrap:wrap;gap:4px">'
        +tags.map(function(t){return '<span style="font-size:10px;padding:2px 8px;border-radius:20px;background:rgba(124,58,237,0.08);color:var(--purple);border:1px solid rgba(124,58,237,0.15);font-family:var(--mono)">'+esc(t)+'</span>';}).join('')
        +'</div></div>';
    }

    html+='</div></div>';
    panel.innerHTML=html;
  }
}

const MODEL_COST_PER_1K = {'gpt-4o-mini':0.000150,'gpt-4o':0.002500,'gpt-4-turbo':0.010000,'gpt-3.5-turbo':0.000500};
const AVG_TOKENS_PER_ENRICH = 800;
function initUsage(){ if(!DB.usage) DB.usage={oaiCalls:0,oaiTokens:0,parCalls:0,parBalance:null}; }
function trackOAICall(t){ initUsage(); DB.usage.oaiCalls++; DB.usage.oaiTokens+=t||AVG_TOKENS_PER_ENRICH; updateUsageUI(); }
function trackParCall(){ initUsage(); DB.usage.parCalls++; updateUsageUI(); }
function updateUsageUI(){
  initUsage();
  const calls=document.getElementById('stat-oai-calls');
  const cost=document.getElementById('stat-oai-cost');
  const pcalls=document.getElementById('stat-par-calls');
}

async function fetchKeyBalance(key){
  try{
    const res=await fetch(PROXY+'/v1/account',{
      headers:{'x-api-key':key.value,'Authorization':'Bearer '+key.value},
      signal:AbortSignal.timeout(8000)
    });
    const txt=await res.text();
    log('Balance raw for "'+key.label+'": HTTP '+res.status+' '+txt.slice(0,300),'gray');
    if(!res.ok) return null;
    let data;
    try{ data=JSON.parse(txt); } catch(e){ return null; }
    var bal=
      data.balance         ??
      data.credits         ??
      data.credits_remaining ??
      data.credit_balance  ??
      data.account_balance ??
      data.remaining_credits ??
      data.available_credits ??
      (data.account?.balance) ??
      (data.account?.credits) ??
      null;
    if(bal===null) log('Balance field not found. Raw keys: '+JSON.stringify(Object.keys(data)),'amber');
    return bal;
  }catch(e){ log('Balance error for "'+key.label+'": '+e.message,'red'); return null; }
}

async function testAllAPIs(){
  const btn=document.getElementById('test-all-btn');
  const results=document.getElementById('api-test-results');
  btn.disabled=true; btn.textContent='Testing...';
  results.innerHTML='';

  function card(name, status, detail, color){
    const colors={ok:'#22c55e',fail:'#ef4444',warn:'#f59e0b',skip:'#6b7280'};
    const icons={ok:'✓',fail:'✗',warn:'⚠',skip:'—'};
    const c=colors[color]||colors.skip;
    const ic=icons[color]||'—';
    results.innerHTML+=`<div style="background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:10px 12px;border-left:3px solid ${c}">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">
        <span style="color:${c};font-weight:700;font-family:var(--mono);font-size:13px">${ic}</span>
        <span style="font-size:11px;font-weight:600;color:var(--text1)">${name}</span>
      </div>
      <div style="font-size:10px;font-family:var(--mono);color:var(--text3);line-height:1.4">${detail}</div>
    </div>`;
  }

  try{
    const key=getActiveKey();
    if(!key){ card('Parallel AI','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await pFetch('GET','/v1alpha/monitors');
      if(r&&!r.__pFetchError){
        card('Parallel AI','ok','✓ Connected — '+( Array.isArray(r)?r.length:0)+' monitor(s) on account','ok');
        log('API Test — Parallel AI: ✓ OK','green');
      } else {
        card('Parallel AI','fail','HTTP '+( r&&r.status)+' — '+(r&&r.body||'').slice(0,60),'fail');
        log('API Test — Parallel AI: ✗ '+( r&&r.status),'red');
      }
    }
  }catch(e){ card('Parallel AI','fail',e.message.slice(0,80),'fail'); }

  try{
    const oaiKey=DB.settings&&DB.settings.oaiKey;
    if(!oaiKey){ card('OpenAI (GPT)','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch(PROXY+'/openai/v1/chat/completions',{
        method:'POST',
        headers:{'Authorization':'Bearer '+oaiKey,'Content-Type':'application/json'},
        body:JSON.stringify({model:DB.settings.oaiModel||'gpt-4o-mini',max_tokens:5,messages:[{role:'user',content:'Say OK'}]}),
        signal:AbortSignal.timeout(10000)
      });
      const d=await r.json();
      if(r.ok&&d.choices){
        card('OpenAI (GPT)','ok','✓ '+( DB.settings.oaiModel||'gpt-4o-mini')+' responding','ok');
        log('API Test — OpenAI: ✓ OK','green');
      } else {
        card('OpenAI (GPT)','fail','HTTP '+r.status+' — '+(d.error&&d.error.message||JSON.stringify(d)).slice(0,60),'fail');
        log('API Test — OpenAI: ✗ '+(d.error&&d.error.message||r.status),'red');
      }
    }
  }catch(e){ card('OpenAI (GPT)','fail',e.message.slice(0,80),'fail'); }

  try{
    const ck=DB.settings&&DB.settings.crustdataKey;
    if(!ck){ card('Crustdata','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch(PROXY+'/crustdata/screener/company?company_domain=google.com&fields=company_name',{
        headers:{'Authorization':'Bearer '+ck,'Accept':'application/json','x-api-version':'2025-11-01'},
        signal:AbortSignal.timeout(10000)
      });
      const txt=await r.text();
      if(r.ok&&!txt.startsWith('<')){
        card('Crustdata','ok','✓ Company enrich responding','ok');
        log('API Test — Crustdata: ✓ OK','green');
      } else {
        card('Crustdata','fail','HTTP '+r.status+' — '+txt.slice(0,60),'fail');
        log('API Test — Crustdata: ✗ HTTP '+r.status,'red');
      }
    }
  }catch(e){ card('Crustdata','fail',e.message.slice(0,80),'fail'); }

  try{
    const feKey=DB.settings&&DB.settings.fullenrichKey;
    if(!feKey){ card('FullEnrich','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch(PROXY+'/fullenrich/api/v1/contact/enrich/bulk',{
        method:'POST',
        headers:{'api-key':feKey,'Content-Type':'application/json'},
        body:JSON.stringify({name:'test',datas:[{firstname:'John',lastname:'Doe',linkedin_url:'https://linkedin.com/in/johndoe',domain:'google.com',enrich_fields:['contact.emails']}]}),
        signal:AbortSignal.timeout(10000)
      });
      const txt=await r.text();
      if(r.ok&&!txt.startsWith('<')){
        card('FullEnrich','ok','✓ Email enrichment API responding','ok');
        log('API Test — FullEnrich: ✓ OK','green');
      } else if(r.status===401||r.status===403){
        card('FullEnrich','fail','Invalid API key — check key in Keys tab','fail');
        log('API Test — FullEnrich: ✗ Invalid key','red');
      } else if(r.status===422){
        card('FullEnrich','ok','✓ API key valid (HTTP 422 = key accepted, test payload rejected normally)','ok');
        log('API Test — FullEnrich: ✓ OK (422 = key valid)','green');
      } else {
        card('FullEnrich','fail','HTTP '+r.status+' — '+txt.slice(0,60),'fail');
        log('API Test — FullEnrich: ✗ HTTP '+r.status,'red');
      }
    }
  }catch(e){ card('FullEnrich','fail',e.message.slice(0,80),'fail'); }

  try{
    const apolloKey=DB.settings&&DB.settings.apolloKey;
    if(!apolloKey){ card('Apollo','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch('https://apollo-proxy-s5uw.onrender.com/apollo/api/v1/auth/health',{
        method:'GET',
        headers:{'X-Api-Key':apolloKey,'Content-Type':'application/json'},
        signal:AbortSignal.timeout(10000)
      });
      const txt=await r.text();
      if(r.ok){
        card('Apollo','ok','✓ People Search API responding','ok');
        log('API Test — Apollo: ✓ OK','green');
      } else if(r.status===401||r.status===403){
        card('Apollo','fail','Invalid API key — must be Master API key','fail');
        log('API Test — Apollo: ✗ Invalid key','red');
      } else {
        card('Apollo','warn','HTTP '+r.status+' — proxy reachable but check key','warn');
        log('API Test — Apollo: ⚠ HTTP '+r.status,'amber');
      }
    }
  }catch(e){ card('Apollo','fail',e.message.slice(0,80),'fail'); }

  try{
    const abKey=getActiveAutoboundKey();
    if(!abKey){ card('Autobound','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch(PROXY+'/autobound/api/external/generate-insights/v1.4',{
        method:'POST',
        headers:{'X-API-KEY':abKey.value,'Content-Type':'application/json'},
        body:JSON.stringify({contactEmail:'test@google.com'}),
        signal:AbortSignal.timeout(10000)
      });
      const txt=await r.text();
      if(r.ok||(r.status===400&&!txt.includes('Api key'))){
        card('Autobound','ok','✓ Insights API responding','ok');
        log('API Test — Autobound: ✓ OK','green');
      } else if(r.status===401||txt.includes('Api key')){
        card('Autobound','fail','Invalid API key (HTTP '+r.status+')','fail');
        log('API Test — Autobound: ✗ Invalid key','red');
      } else {
        card('Autobound','fail','HTTP '+r.status+' — '+txt.slice(0,60),'fail');
        log('API Test — Autobound: ✗ HTTP '+r.status,'red');
      }
    }
  }catch(e){ card('Autobound','fail',e.message.slice(0,80),'fail'); }

  try{
    const slKey=DB.outreach&&DB.outreach.smartleadKey;
    if(!slKey){ card('Smartlead','skip','No key set — add in Keys tab','skip'); }
    else{
      const r=await fetch(PROXY+'/smartlead/api/v1/campaigns?api_key='+encodeURIComponent(slKey),{signal:AbortSignal.timeout(10000)});
      const txt=await r.text();
      if(r.ok&&!txt.startsWith('<')){
        const data=JSON.parse(txt);
        const count=Array.isArray(data)?data.length:(data.list||[]).length;
        card('Smartlead','ok','✓ Connected — '+count+' campaign(s)','ok');
        log('API Test — Smartlead: ✓ OK ('+count+' campaigns)','green');
      } else if(r.status===401||r.status===403){
        card('Smartlead','fail','Invalid API key (HTTP '+r.status+')','fail');
        log('API Test — Smartlead: ✗ Invalid key','red');
      } else {
        card('Smartlead','fail','HTTP '+r.status+' — '+txt.slice(0,60),'fail');
        log('API Test — Smartlead: ✗ HTTP '+r.status,'red');
      }
    }
  }catch(e){ card('Smartlead','fail',e.message.slice(0,80),'fail'); }

  try{
    var hrKeyTest=DB.heyreachKeys&&DB.heyreachKeys.find(function(k){return k.active;});
    if(!hrKeyTest){ card('HeyReach','skip','No key set — add in Keys tab','skip'); }
    else{
      var rHr=await fetch(PROXY+'/heyreach/api/public/campaign/GetAll',{
        method:'POST',
        headers:{'X-API-KEY':hrKeyTest.value,'Content-Type':'application/json','Accept':'application/json'},
        body:JSON.stringify({}),
        signal:AbortSignal.timeout(10000)
      });
      var txtHr=await rHr.text();
      if(rHr.ok){
        var hrCampsTest=[];try{hrCampsTest=JSON.parse(txtHr);}catch(e){}
        card('HeyReach','ok','Connected — '+(Array.isArray(hrCampsTest)?hrCampsTest.length:0)+' campaign(s)','ok');
        log('API Test — HeyReach: OK','green');
      } else if(rHr.status===401||rHr.status===403){
        card('HeyReach','fail','Invalid API key','fail');
        log('API Test — HeyReach: Invalid key','red');
      } else {
        card('HeyReach','fail','HTTP '+rHr.status+' — '+txtHr.slice(0,60),'fail');
        log('API Test — HeyReach: HTTP '+rHr.status,'red');
      }
    }
  }catch(eHr){ card('HeyReach','fail',eHr.message.slice(0,80),'fail'); }

  try{
    const r=await fetch(PROXY+'/kv/dmand_v2',{signal:AbortSignal.timeout(8000)});
    if(r.ok||r.status===404){
      card('Cloudflare KV','ok','✓ KV storage reachable'+(r.status===404?' (no data yet)':''),'ok');
      log('API Test — Cloudflare KV: ✓ OK','green');
    } else {
      card('Cloudflare KV','fail','HTTP '+r.status+' — check Worker deployment','fail');
      log('API Test — Cloudflare KV: ✗ HTTP '+r.status,'red');
    }
  }catch(e){ card('Cloudflare KV','fail',e.message.slice(0,80),'fail'); }

  btn.disabled=false; btn.textContent='⚡ Test All';
  log('── API Test complete ──','blue');
}

function resetUsageStats(){
  if(!confirm('Reset all usage counters?')) return;
  DB.usage={oaiCalls:0,oaiTokens:0,parCalls:0,parBalance:DB.usage?.parBalance||null};
  save(); updateUsageUI(); showAlert('Usage counters reset.','info');
}

function saveOAIKey(){
  const val=document.getElementById('oai-key').value.trim();
  const model=document.getElementById('oai-model').value;
  if(!val){showAlert('Enter your OpenAI API key.','error');return;}
  DB.settings.oaiKey=val; DB.settings.oaiModel=model;
  save();
  document.getElementById('oai-status').textContent='Saved ✓ model: '+model;
  showAlert('OpenAI key saved.','success');
}
function loadOAIKeyUI(){
  if(DB.settings.oaiKey){ document.getElementById('oai-key').value=DB.settings.oaiKey; document.getElementById('oai-model').value=DB.settings.oaiModel||'gpt-4o-mini'; document.getElementById('oai-status').textContent='Active ✓'; }
}

const MAX_LOG=500;
var _logSaveTimer=null;
var _logUnsaved=0;
function log(msg,type='gray'){
  const item={t:new Date().toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:true}),msg,type};
  if(!DB.log) DB.log=[];
  DB.log.unshift(item);
  if(DB.log.length>MAX_LOG) DB.log.length=MAX_LOG;
  renderLog();
  _logUnsaved++;
  if(_logUnsaved>=10){
    _logUnsaved=0;
    clearTimeout(_logSaveTimer);
    _logSaveTimer=setTimeout(function(){
      if(cloudSyncEnabled) cloudSaveLogs().catch(function(){});
    },5000);
  }
}
function renderLog(){
  const el=document.getElementById('activity-log-list');
  if(!el) return;
  var countEl=document.getElementById('log-count');
  if(countEl && DB.log && DB.log.length) countEl.textContent='('+DB.log.length+')';
  if(!DB.log||!DB.log.length){ el.innerHTML='<div style="padding:6px 20px;font-size:10px;color:var(--text3);font-family:var(--mono)">No activity yet</div>'; return; }
  el.innerHTML=DB.log.map(l=>`<div class="log-item"><div class="log-dot log-dot-${l.type}"></div><span class="log-time">${l.t}</span><span class="log-msg">${esc(l.msg)}</span></div>`).join('');
}
function clearLog(){
  DB.log=[]; save(); renderLog();
  if(cloudSyncEnabled&&PROXY&&PROXY!==DEFAULT_PROXY){
    fetch(PROXY+'/kv/dmand_logs',{method:'DELETE',signal:AbortSignal.timeout(5000)}).catch(function(){});
  }
  var countEl=document.getElementById('log-count');
  if(countEl) countEl.textContent='';
}
function copyLog(){
  var text=(DB.log||[]).slice().reverse().map(function(e){
    return e.t+' '+e.msg;
  }).join('\n');
  if(!text){showAlert('Activity log is empty.','info');return;}
  navigator.clipboard.writeText(text).then(function(){
    var btn=document.getElementById('copy-log-btn');
    if(btn){btn.textContent='copied ✓';setTimeout(function(){btn.textContent='copy';},2000);}
  }).catch(function(){
    var ta=document.createElement('textarea');
    ta.value=text; ta.style.position='fixed'; ta.style.opacity='0';
    document.body.appendChild(ta); ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    var btn=document.getElementById('copy-log-btn');
    if(btn){btn.textContent='copied ✓';setTimeout(function(){btn.textContent='copy';},2000);}
  });
}

window.resetAllEnrichment=function(){ DB.events.forEach(e=>e.enrichment=null); save(); renderEvents(); console.log('All enrichments reset.'); };

function addHeyreachKey(){
  var val=document.getElementById('new-hr-key').value.trim();
  var label=document.getElementById('new-hr-label').value.trim()||('Workspace #'+(DB.heyreachKeys.length+1));
  if(!val){showAlert('Enter a HeyReach API key.','error');return;}
  if(!DB.heyreachKeys) DB.heyreachKeys=[];
  DB.heyreachKeys.push({id:'hrk_'+Date.now(),value:val,label,active:true});
  document.getElementById('new-hr-key').value='';
  document.getElementById('new-hr-label').value='';
  save(); loadHeyreachKeysUI(); updateHrSidebarDot();
  showAlert('HeyReach key "'+label+'" added.','success');
}
function removeHeyreachKey(id){
  DB.heyreachKeys=(DB.heyreachKeys||[]).filter(function(k){return k.id!==id;});
  save(); loadHeyreachKeysUI();
}
function getActiveHeyreachKey(){
  if(!DB.heyreachKeys||!DB.heyreachKeys.length) return null;
  return DB.heyreachKeys.find(function(k){return k.active!==false;})||null;
}
function loadHeyreachKeysUI(){
  if(!DB.heyreachKeys) DB.heyreachKeys=[];
  var el=document.getElementById('heyreach-key-list');
  if(!el) return;
  if(!DB.heyreachKeys.length){
    el.innerHTML='<div style="font-size:11px;color:var(--text3);font-family:var(--mono)">No HeyReach keys added yet.</div>';
    var st=document.getElementById('hr-key-status');
    if(st) st.textContent='';
    return;
  }
  el.innerHTML=DB.heyreachKeys.map(function(k){
    return '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;background:rgba(10,102,194,0.05);border:1px solid rgba(10,102,194,0.2);border-radius:6px">'
      +'<div style="width:8px;height:8px;border-radius:50%;background:#0a66c2;flex-shrink:0"></div>'
      +'<div style="flex:1;min-width:0">'
      +'<div style="font-size:12px;font-weight:600;color:var(--text)">'+esc(k.label)+'<span style="font-size:10px;font-family:var(--mono);color:var(--text3);margin-left:8px">'+k.value.slice(0,12)+'…</span></div>'
      +'</div>'
      +'<button class="btn btn-sm btn-danger" onclick="removeHeyreachKey(\''+k.id+'\')">Remove</button>'
      +'</div>';
  }).join('');
  var st=document.getElementById('hr-key-status');
  if(st) st.textContent=DB.heyreachKeys.length+' key(s) active';
}

async function refreshHeyreachCampaigns(){
  var key=getActiveHeyreachKey();
  if(!key){showAlert('Add a HeyReach API key in the Keys tab first.','error');return;}
  var el=document.getElementById('campaign-list');
  if(el) el.innerHTML='<div style="text-align:center;padding:30px;font-size:12px;font-family:var(--mono);color:#0a66c2">💼 Loading campaigns from HeyReach...</div>';
  log('HeyReach: fetching campaigns...','blue');
  try{
    var r=await fetch(PROXY+'/heyreach/api/public/campaign/GetAll',{
      method:'POST',
      headers:{'X-API-KEY':key.value,'Content-Type':'application/json','Accept':'application/json'},
      body:JSON.stringify({offset:0,limit:100}),
      signal:AbortSignal.timeout(15000)
    });
    var txt=await r.text();
    log('HeyReach campaigns: HTTP '+r.status+' — '+txt.slice(0,120),'gray');
    if(!r.ok){log('HeyReach error: '+txt.slice(0,200),'red');showAlert('HeyReach error: HTTP '+r.status,'error');renderCampaignList();return;}
    var data; try{data=JSON.parse(txt);}catch(e){showAlert('HeyReach: invalid response','error');renderCampaignList();return;}
    var list=Array.isArray(data)?data:(data.items||data.campaigns||data.data||[]);
    if(!DB.hrCampaigns) DB.hrCampaigns=[];
    list.forEach(function(hc){
      var existing=DB.hrCampaigns.find(function(c){return String(c.hr_id)===String(hc.id);});
      if(existing){
        existing.hr_name=hc.name||existing.hr_name;
        existing.hr_status=hc.status||existing.hr_status;
      } else {
        DB.hrCampaigns.push({
          hr_id:hc.id,
          hr_name:hc.name||('HeyReach Campaign '+hc.id),
          hr_status:hc.status||'ACTIVE',
          hr_mode:'',               // default unassigned — user sets in Campaigns tab
          hr_example_steps:{CM:'',DM1:'',DM2:'',DM3:'',DM4:''},
          hr_created_at:hc.creationTime||new Date().toISOString(),
          _expanded:false
        });
      }
    });
    save(); renderCampaignList();
    document.getElementById('nb-campaigns').textContent=(DB.campaigns.length+DB.hrCampaigns.length);
    showAlert('Loaded '+list.length+' HeyReach campaign(s)','success');
    log('HeyReach: '+list.length+' campaign(s) synced','green');
  }catch(e){
    log('HeyReach sync error: '+e.message,'red');
    showAlert('HeyReach sync error: '+e.message,'error');
    renderCampaignList();
  }
}

function setHrCampaignMode(hrId, mode){
  var c=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrId);});
  if(c){ c.hr_mode=mode; save(); }
}

function toggleHrCampCard(hrId){
  var c=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrId);});
  if(!c) return;
  c._expanded=!c._expanded;
  var body=document.getElementById('hr-camp-body-'+hrId);
  if(body) body.style.display=c._expanded?'block':'none';
}

function saveHrCampExamples(hrId){
  var c=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrId);});
  if(!c) return;
  var keys=['CM','DM1','DM2','DM3','DM4'];
  if(!c.hr_example_steps) c.hr_example_steps={};
  keys.forEach(function(k){
    var el=document.getElementById('hr-ex-'+hrId+'-'+k);
    if(el) c.hr_example_steps[k]=el.value.trim();
  });
  save();
  var st=document.getElementById('hr-ex-status-'+hrId);
  if(st){st.textContent='✓ Saved';setTimeout(function(){st.textContent='';},2000);}
  log('HeyReach examples saved for "'+c.hr_name+'"','green');
}

function removeHrCampaignLocal(hrId){
  DB.hrCampaigns=DB.hrCampaigns.filter(function(c){return String(c.hr_id)!==String(hrId);});
  save(); renderCampaignList();
}

function setCampFilter(f){
  campFilter=f;
  ['all','smartlead','heyreach'].forEach(function(id){
    var btn=document.getElementById('camp-filter-'+id);
    if(btn) btn.classList.toggle('active',id===f);
  });
  renderCampaignList();
}

async function generateLinkedInMessagesForContact(contact, sig, mode){
  var oaiKey=DB.settings.oaiKey;
  if(!oaiKey){log('LinkedIn gen: no OpenAI key','red');return null;}
  var firstName=(contact.name||'').split(' ')[0]||'there';
  var icebreaker=buildInsightsSummary(contact)||'';
  var hrCamp=null;
  var sigObj=(sig&&sig.id)?sig:(DB.signals.find(function(s){return s.id===contact.signal_id;})||null);
  if(sigObj&&sigObj.icp_targets){
    var t=sigObj.icp_targets.find(function(x){return x.persona_id===contact.icp_persona_id||x.persona_name===contact.icp_persona_name;});
    if(t&&t.hr_campaign_id) hrCamp=DB.hrCampaigns.find(function(c){return String(c.hr_id)===String(t.hr_campaign_id);});
  }
  var modeInstr={
    'signal':'CM: short connection note referencing the signal event naturally. Max 250 chars.\nDM1: open with the signal/trigger event as hook, bridge to value prop.',
    'icebreaker':'CM: short connection note referencing their recent LinkedIn activity. Max 250 chars.\nDM1: open with icebreaker (their LinkedIn activity) as personal hook, then mention signal as business context, bridge to value prop.',
    'signal_ib':'CM: short connection note referencing their recent LinkedIn activity. Max 250 chars.\nDM1: open with icebreaker (their LinkedIn activity) as personal hook, then mention signal as business context, then bridge to value prop.'
  };
  var exRef='';
  if(hrCamp&&hrCamp.hr_example_steps){
    var ex=hrCamp.hr_example_steps;
    var hasEx=Object.values(ex).some(function(v){return v&&v.length>3;});
    if(hasEx){
      exRef='\nTone guide (match style, do NOT copy):\n';
      ['CM','DM1','DM2','DM3'].forEach(function(k){
        if(ex[k]) exRef+=k+': '+ex[k].slice(0,100)+'…\n';
      });
    }
  }
  var systemPrompt='B2B LinkedIn outreach copywriter. Return ONLY valid JSON with keys: CM, DM1, DM2, DM3, DM4.\n'
    +'Rules: first name only, no sign-off, no emojis, human tone, max 250 chars for CM.\n'
    +(modeInstr[mode]||modeInstr['signal'])+'\n'
    +'DM2: value/insight follow-up (~80 words). DM3: question-based follow-up (~60 words). DM4: breakup message, honest, no pressure (~50 words).\n'
    +'Company: '+(DB.outreach?.companyBrief||'').slice(0,150)+'\n'
    +'Value prop: '+(DB.outreach?.valueProp||'').slice(0,100)+'\n'
    +'Pain: '+(DB.outreach?.painPoints||'').slice(0,100)+'\n'
    +'CTA: '+(DB.outreach?.cta||'Book a 15-min call')+exRef;
  var userPrompt='Write a LinkedIn CM+DM1-DM5 sequence for:\n'
    +'- First name: '+firstName+'\n'
    +'- Title: '+(contact.title||'')+'\n'
    +'- Company: '+(contact.company||'')+'\n'
    +'- Signal: '+(contact.event_brief||contact.signal_name||'')+'\n'
    +(icebreaker?'- Recent LinkedIn activity:\n'+icebreaker:'');
  log('LinkedIn gen: calling GPT for '+contact.name+' (mode: '+mode+')','blue');
  try{
    var res=await fetch(PROXY+'/openai/v1/chat/completions',{
      method:'POST',
      headers:{'Content-Type':'application/json','Authorization':'Bearer '+oaiKey},
      body:JSON.stringify({model:DB.settings.oaiModel||'gpt-4o-mini',max_tokens:750,messages:[{role:'system',content:systemPrompt},{role:'user',content:userPrompt}]}),
      signal:AbortSignal.timeout(35000)
    });
    var data=await res.json();
    if(!res.ok){log('LinkedIn gen GPT error: '+(data.error&&data.error.message||'unknown'),'red');return null;}
    var raw=data.choices[0].message.content.trim();
    var parsed=JSON.parse(raw.replace(/```json|```/g,'').trim());
    if(parsed.CM) parsed.CM=parsed.CM.slice(0,295);
    contact.linkedin_messages={...parsed,mode,generated_at:new Date().toISOString()};
    save();
    log('LinkedIn gen: ✓ messages written for '+contact.name,'green');
    return parsed;
  }catch(e){
    log('LinkedIn gen error for '+contact.name+': '+e.message,'red');
    showAlert('LinkedIn message generation failed: '+e.message,'error',5000);
    return null;
  }
}

async function pushToHeyreach(contact, hrCampaignId, hrCampaignName, messages){
  var key=getActiveHeyreachKey();
  if(!key){log('HeyReach: no API key — add in Keys tab','red');return;}
  if(!contact.linkedin){log('HeyReach: no LinkedIn URL for '+contact.name,'amber');return;}
  if(!messages||!messages.CM){log('HeyReach: no messages generated for '+contact.name,'red');return;}
  var nameParts=(contact.name||'').split(' ');
  var payload={
    campaignId:parseInt(hrCampaignId),
    accountLeadPairs:[{
      linkedInAccountId:null,
      lead:{
        profileUrl:contact.linkedin,
        firstName:nameParts[0]||'',
        lastName:nameParts.slice(1).join(' ')||'',
        position:contact.title||'',
        companyName:contact.company||'',
        location:contact.location||'',
        emailAddress:contact.business_email&&contact.business_email!=='N/A'?contact.business_email:'',
        customUserFields:[
          {name:'CM',  value:(messages.CM ||'').slice(0,295)},
          {name:'DM1', value:(messages.DM1||'').slice(0,2000)},
          {name:'DM2', value:(messages.DM2||'').slice(0,2000)},
          {name:'DM3', value:(messages.DM3||'').slice(0,2000)},
          {name:'DM4', value:(messages.DM4||'').slice(0,2000)}
        ]
      }
    }]
  };
  try{
  var hrCamp=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrCampaignId);});
  log('HeyReach pushing '+contact.name+' → campaign '+hrCampaignId+' ('+( hrCamp?hrCamp.hr_name:'?')+') status='+(hrCamp?hrCamp.hr_status:'?'),'gray');
  log('HeyReach payload preview: profileUrl='+contact.linkedin.slice(0,60)+' CM='+((messages.CM||'').slice(0,40))+'...','gray');
  if(hrCamp&&hrCamp.hr_status!=='ACTIVE'&&hrCamp.hr_status!=='IN_PROGRESS'){
    log('HeyReach WARNING: campaign status is '+hrCamp.hr_status+' — AddLeadsToCampaignV2 requires ACTIVE or IN_PROGRESS','amber');
  }
    var r=await fetch(PROXY+'/heyreach/api/public/campaign/AddLeadsToCampaignV2',{
      method:'POST',
      headers:{'X-API-KEY':key.value,'Content-Type':'application/json','Accept':'application/json'},
      body:JSON.stringify(payload),
      signal:AbortSignal.timeout(20000)
    });
    var txt=await r.text();
    log('HeyReach push '+contact.name+': HTTP '+r.status+' '+txt.slice(0,200),(r.ok?'green':'red'));
    var d={};
    try{ if(txt&&txt.trim()) d=JSON.parse(txt); }catch(e){  }
    if(r.ok&&(d.addedCount>0||d.addedLeadsCount>0||d.updatedLeadsCount>0||d.totalCount>0||d.ok||r.status===201||(r.status===200&&!d.errors&&!d.failedLeadsCount))||r.status===204){
      contact.heyreach_campaign_id=hrCampaignId;
      contact.heyreach_campaign_name=hrCampaignName||'HeyReach Campaign';
      contact.heyreach_pushed_at=new Date().toISOString();
      contact.linkedin_status='pending';
      var resultMsg=d.addedCount>0?'added':d.addedLeadsCount>0?'added':d.updatedLeadsCount>0?'updated':'pushed';
      log('── HeyReach ✓: '+contact.name+' → "'+hrCampaignName+'" | linkedin: '+contact.linkedin.slice(0,40),'green');
      showAlert('💼 '+contact.name+' pushed to LinkedIn campaign "'+hrCampaignName+'"','success',4000);
    } else if(d.failedLeadsCount>0){
      log('HeyReach: lead already in campaign or rejected for '+contact.name,'amber');
      if(d.failedLeadsCount&&!d.addedLeadsCount){
        contact.heyreach_campaign_id=hrCampaignId;
        contact.heyreach_campaign_name=hrCampaignName||'HeyReach Campaign';
      }
    } else {
      log('HeyReach: unexpected response for '+contact.name+': '+JSON.stringify(d).slice(0,150),'amber');
    }
    save(); renderContacts(false);
  }catch(e){
    log('HeyReach push error for '+contact.name+': '+e.message,'red');
  }
}

function openLiModal(contactId, hrCampaignId){
  var c=DB.contacts.find(function(x){return x.id===contactId;});
  if(!c){return;}
  if(!c.linkedin){showAlert('Contact has no LinkedIn URL.','warning');return;}
  var hrKey=getActiveHeyreachKey();
  if(!hrKey){showAlert('Add a HeyReach API key in the Keys tab first.','error');return;}

  var hrId=hrCampaignId;
  var hrName='';
  if(!hrId){
    var sig2=DB.signals.find(function(s){return s.id===c.signal_id;});
    var t2=sig2&&sig2.icp_targets&&sig2.icp_targets.find(function(t){return t.persona_id===c.icp_persona_id||t.persona_name===c.icp_persona_name;});
    hrId=t2&&t2.hr_campaign_id;
    hrName=t2&&t2.hr_campaign_name||'';
  }
  var hrCamp=hrId?(DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrId);})||{}):null;
  hrName=hrCamp&&hrCamp.hr_name||hrName||'';

  if(!hrId||!hrCamp){
    openLiModalNoCampaign(contactId);
    return;
  }

  liModalContactId=contactId;
  liModalContactIds=[contactId];
  liModalCampaignId=hrId;
  liModalCampaignName=hrName;
  liGeneratedMessages=null;

  document.getElementById('li-modal-title').textContent='LinkedIn Push — '+(c.name||'Contact');
  document.getElementById('li-modal-sub').textContent=(c.title||'')+(c.company?' · '+c.company:'');
  document.getElementById('li-modal-camp-name').textContent=hrName;
  var modeLabels={'signal':'📡 Signal','signal_ib':'🧊 Signal + IB'};
  document.getElementById('li-modal-mode-badge').textContent=modeLabels[hrCamp.hr_mode||'signal']||hrCamp.hr_mode||'Signal';
  document.getElementById('li-modal-msgs-badge').textContent='5 messages (CM + DM1–DM4)';
  document.getElementById('li-send-row').style.display='none';
  document.getElementById('li-gen-status').textContent='';
  document.getElementById('li-send-status').textContent='';

  if(c.linkedin_messages&&c.linkedin_messages.CM){
    liGeneratedMessages=c.linkedin_messages;
    renderLiMsgPreview(c.linkedin_messages);
    document.getElementById('li-send-row').style.display='block';
    document.getElementById('btn-generate-li').textContent='✨ Regenerate';
  } else {
    document.getElementById('li-steps-container').innerHTML='<div class="empty" style="margin:24px 0"><div class="empty-icon" style="font-size:24px">💼</div><div class="empty-title" style="font-size:13px">Click Generate to write your LinkedIn sequence</div><div class="empty-sub" style="font-size:11px">Mode: '+(modeLabels[hrCamp.hr_mode||'signal']||'Signal')+'</div></div>';
    document.getElementById('btn-generate-li').textContent='✨ Generate LinkedIn sequence';
  }

  document.getElementById('li-modal-overlay').style.display='block';
  document.getElementById('li-modal').style.display='block';
  setTimeout(function(){
    var genBtn=document.getElementById('btn-generate-li');
    if(genBtn&&!genBtn.disabled) genBtn.click();
  },200);
}

function openLiModalWithCampaign(contactId, hrCampaignId){
  if(!hrCampaignId) return;
  openLiModal(contactId, hrCampaignId);
}

function openLiModalNoCampaign(contactId){
  var c=DB.contacts.find(function(x){return x.id===contactId;});
  if(!c) return;
  var hrCampsAvail=(DB.hrCampaigns||[]).filter(function(hc){return hc.hr_mode&&hc.hr_mode!=='';});
  if(!hrCampsAvail.length){
    showAlert('No assigned HeyReach campaigns. Set a mode (Signal/Signal+IB/Manual) in the Campaigns tab first.','warning');
    return;
  }
  liModalContactId=contactId;
  liModalContactIds=[contactId];
  liModalCampaignId=null;
  liModalCampaignName='';
  liGeneratedMessages=null;

  document.getElementById('li-modal-title').textContent='LinkedIn Push — '+(c.name||'Contact');
  document.getElementById('li-modal-sub').textContent=(c.title||'')+(c.company?' · '+c.company:'');

  var campPickerHtml='<select id="li-camp-picker" onchange="onLiCampPick(this.value)" style="font-size:11px;font-family:var(--mono);padding:3px 8px;border:1px solid rgba(10,102,194,0.3);border-radius:4px;color:#0a66c2;background:var(--surface);cursor:pointer">'
    +'<option value="">— Select LinkedIn campaign —</option>'
    +hrCampsAvail.map(function(hc){return '<option value="'+hc.hr_id+'">'+esc(hc.hr_name)+'</option>';}).join('')
    +'</select>';
  document.getElementById('li-modal-camp-name').innerHTML=campPickerHtml;
  document.getElementById('li-modal-mode-badge').textContent='Pick a campaign';
  document.getElementById('li-modal-msgs-badge').textContent='5 messages (CM + DM1–DM4)';
  document.getElementById('li-send-row').style.display='none';
  document.getElementById('li-gen-status').textContent='';
  document.getElementById('li-steps-container').innerHTML='<div class="empty" style="margin:24px 0"><div class="empty-icon" style="font-size:24px">💼</div><div class="empty-title" style="font-size:13px">Select a LinkedIn campaign above, then generate</div></div>';
  document.getElementById('btn-generate-li').textContent='✨ Generate LinkedIn sequence';

  document.getElementById('li-modal-overlay').style.display='block';
  document.getElementById('li-modal').style.display='block';
}

function onLiCampPick(hrId){
  if(!hrId) return;
  var hc=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(hrId);});
  if(!hc) return;
  liModalCampaignId=hrId;
  liModalCampaignName=hc.hr_name;
  document.getElementById('li-modal-camp-name').textContent=hc.hr_name;
  var modeLabels={'signal':'📡 Signal','signal_ib':'🧊 Signal+IB','':`— Unassigned`};
  document.getElementById('li-modal-mode-badge').textContent=modeLabels[hc.hr_mode||'']||'Signal';
  document.getElementById('li-gen-status').textContent='Campaign selected — click Generate';
}

function closeLiModal(){
  document.getElementById('li-modal-overlay').style.display='none';
  document.getElementById('li-modal').style.display='none';
  liModalContactId=null; liModalContactIds=[]; liModalCampaignId=null; liGeneratedMessages=null;
}

function renderLiMsgPreview(msgs){
  var keys=['CM','DM1','DM2','DM3','DM4'];
  var labels={CM:'Connection Message (CM) — max 295 chars',DM1:'DM1 — First message after connect',DM2:'DM2 — Follow-up #1',DM3:'DM3 — Follow-up #2 (question)',DM4:'DM4 — Breakup message'};
  var html=keys.map(function(k){
    var val=msgs[k]||'';
    var charCount=val.length;
    var warn=k==='CM'&&charCount>280?'<span style="color:var(--red);font-size:10px;margin-left:6px">⚠ '+charCount+'/295 chars</span>':'';
    return '<div class="li-msg-card">'
      +'<div class="li-msg-card-label">'+labels[k]+(k==='CM'?'<span style="font-size:9px;color:var(--text3);font-weight:400;margin-left:6px">'+charCount+' chars</span>':'')+warn+'</div>'
      +'<textarea class="form-input" id="li-msg-'+k+'" rows="'+(k==='CM'?2:4)+'" style="font-size:12px;resize:vertical;line-height:1.55">'+esc(val)+'</textarea>'
      +'</div>';
  }).join('');
  document.getElementById('li-steps-container').innerHTML=html;
}

async function generateLinkedInMessages(){
  var c=DB.contacts.find(function(x){return x.id===liModalContactId;});
  if(!c) return;
  if(!DB.settings.oaiKey){showAlert('OpenAI key required for generation.','error');return;}
  if(!liModalCampaignId){showAlert('Select a LinkedIn campaign first.','warning');return;}
  var btn=document.getElementById('btn-generate-li');
  var status=document.getElementById('li-gen-status');
  btn.disabled=true; btn.textContent='Writing…';
  if(status) status.textContent='GPT is writing your LinkedIn sequence…';
  document.getElementById('li-steps-container').innerHTML='<div style="text-align:center;padding:30px"><div class="spinner" style="margin:0 auto 10px;border-top-color:#0a66c2"></div><div style="font-size:12px;font-family:var(--mono);color:var(--text3)">Writing CM + DM1–DM5…</div></div>';
  var hrCamp=DB.hrCampaigns.find(function(x){return String(x.hr_id)===String(liModalCampaignId);})||{};
  var mode=hrCamp.hr_mode||'signal';
  var sig=DB.signals.find(function(s){return s.id===c.signal_id;})||{};
  var msgs=await generateLinkedInMessagesForContact(c,sig,mode);
  if(msgs){
    liGeneratedMessages=msgs;
    renderLiMsgPreview(msgs);
    document.getElementById('li-send-row').style.display='block';
    if(status) status.textContent='✓ Generated';
    setTimeout(function(){if(status) status.textContent='';},3000);
  } else {
    document.getElementById('li-steps-container').innerHTML='<div style="color:var(--red);font-size:12px;padding:16px">Generation failed. Check OpenAI key and try again.</div>';
    if(status) status.textContent='';
  }
  btn.disabled=false; btn.textContent='✨ Regenerate';
}

async function pushToHeyreachFromModal(){
  var c=DB.contacts.find(function(x){return x.id===liModalContactId;});
  if(!c) return;
  var statusEl=document.getElementById('li-send-status');
  var btn=document.querySelector('#li-send-row .btn-accent');
  if(btn){btn.disabled=true;btn.textContent='Pushing…';}
  var msgs={};
  ['CM','DM1','DM2','DM3','DM4'].forEach(function(k){
    var el=document.getElementById('li-msg-'+k);
    msgs[k]=el?el.value.trim():'';
  });
  msgs.mode=liGeneratedMessages&&liGeneratedMessages.mode||'signal';
  msgs.generated_at=liGeneratedMessages&&liGeneratedMessages.generated_at||new Date().toISOString();
  c.linkedin_messages=msgs;
  save();
  if(statusEl) statusEl.textContent='Pushing to HeyReach…';
  await pushToHeyreach(c, liModalCampaignId, liModalCampaignName, msgs);
  if(statusEl) statusEl.textContent=c.heyreach_campaign_id?'✓ Pushed to LinkedIn!':'✗ Push failed — check Activity log';
  if(btn){btn.disabled=false;btn.textContent='🚀 Push to HeyReach →';}
}

function renderHrCampaignCard(hrc){
  var cid=String(hrc.hr_id);
  var sc=hrc.hr_status==='ACTIVE'?'var(--green)':hrc.hr_status==='PAUSED'?'var(--amber)':'var(--text3)';
  var dmandLeads=(DB.contacts||[]).filter(function(c){return String(c.heyreach_campaign_id)===cid;}).length;
  var modeLabels={'signal':'📡 Signal','signal_ib':'🧊 Signal+IB'};
  var ex=hrc.hr_example_steps||{};
  var hasEx=Object.values(ex).some(function(v){return v&&v.length>3;});
  var isExpanded=hrc._expanded||false;
  var varRef='<div style="background:rgba(10,102,194,0.04);border:1px solid rgba(10,102,194,0.15);border-radius:6px;padding:10px 12px;margin-top:10px">'
    +'<div style="font-size:9px;font-family:var(--mono);color:#0a66c2;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px">Variable reference — type these EXACTLY in your HeyReach sequence</div>'
    +'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px">'
    +['CM','DM1','DM2','DM3','DM4'].map(function(k){
      var descs={CM:'Connection note (≤295 chars)',DM1:'1st msg after connect',DM2:'Follow-up #1',DM3:'Follow-up #2 (question)',DM4:'Breakup message'};
      return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:5px 8px">'
        +'<div style="font-size:11px;font-family:var(--mono);font-weight:700;color:#0a66c2">{{'+k+'}}</div>'
        +'<div style="font-size:9px;color:var(--text3);margin-top:2px">'+descs[k]+'</div>'
        +'</div>';
    }).join('')
    +'</div></div>';
  var exFields=['CM','DM1','DM2','DM3','DM4'].map(function(k){
    return '<div style="margin-bottom:8px">'
      +'<label style="font-size:11px;color:var(--text2);display:block;margin-bottom:3px;font-family:var(--mono)">{{'+k+'}} example</label>'
      +'<textarea id="hr-ex-'+cid+'-'+k+'" class="form-input" rows="2" style="font-size:11px;resize:vertical" placeholder="Write example for '+k+'...">'+esc(ex[k]||'')+'</textarea>'
      +'</div>';
  }).join('');
  return '<div style="background:var(--surface2);border:1px solid rgba(10,102,194,0.2);border-radius:8px;margin-bottom:8px;overflow:hidden;border-left:3px solid #0a66c2">'
    +'<div data-hrid="'+cid+'" onclick="toggleHrCampCard(this.dataset.hrid)" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 16px;cursor:pointer;user-select:none">'
      +'<div style="width:8px;height:8px;border-radius:50%;background:'+sc+';flex-shrink:0"></div>'
      +'<div style="font-size:12px;font-weight:700;flex:1;min-width:0;color:var(--text)">💼 '+esc(hrc.hr_name)+'</div>'
      +'<span style="font-size:9px;font-family:var(--mono);padding:2px 7px;border-radius:3px;background:'+sc+'22;color:'+sc+'">'+esc(hrc.hr_status||'UNKNOWN')+'</span>'
      +'<span style="font-size:9px;font-family:var(--mono);color:var(--text3)">Dmand: '+dmandLeads+' pushed</span>'
      +'<select onclick="event.stopPropagation()" onchange="setHrCampaignMode(\''+cid+'\',this.value)" style="font-size:11px;font-family:var(--mono);padding:3px 7px;background:var(--surface);border:1px solid rgba(10,102,194,0.25);color:#0a66c2;border-radius:4px;cursor:pointer">'
        +'<option value="" '+((!hrc.hr_mode||hrc.hr_mode==='')?'selected':'')+'>— Unassigned</option>'
        +'<option value="signal" '+(hrc.hr_mode==='signal'?'selected':'')+'>📡 Signal</option>'
        +'<option value="signal_ib" '+(hrc.hr_mode==='signal_ib'?'selected':'')+'>🧊 Signal+IB</option>'
        +'</select>'
      +'<button onclick="event.stopPropagation();removeHrCampaignLocal(\''+cid+'\')" class="btn btn-sm btn-ghost">Remove</button>'
      +'<span style="font-size:12px;color:var(--text3);margin-left:4px">'+(isExpanded?'▲':'▼')+'</span>'
    +'</div>'
    +'<div id="hr-camp-body-'+cid+'" style="display:'+(isExpanded?'block':'none')+';padding:0 16px 14px 16px;border-top:1px solid rgba(10,102,194,0.1)">'
      +varRef
      +'<div style="margin-top:12px;border-top:1px solid var(--border);padding-top:10px">'
        +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
          +'<div style="font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;color:var(--text3)">'+(hasEx?'✓ Tone examples saved':'Tone examples for GPT (optional)')+'</div>'
          +'<button onclick="event.stopPropagation();toggleHrExamples(\''+cid+'\')" id="hr-ex-toggle-'+cid+'" style="font-size:10px;font-family:var(--mono);background:none;border:none;color:#0a66c2;cursor:pointer;padding:0">'+(hasEx?'Edit examples':'+ Add examples')+'</button>'
        +'</div>'
        +'<div id="hr-ex-fields-'+cid+'" style="display:none">'
          +exFields
          +'<div style="display:flex;align-items:center;gap:8px;margin-top:6px">'
            +'<button class="btn btn-sm" onclick="saveHrCampExamples(\''+cid+'\')" style="border-color:rgba(10,102,194,0.3);color:#0a66c2">Save examples →</button>'
            +'<span id="hr-ex-status-'+cid+'" style="font-size:11px;font-family:var(--mono);color:var(--green)"></span>'
          +'</div>'
        +'</div>'
      +'</div>'
    +'</div>'
  +'</div>';
}

function toggleHrExamples(cid){
  var el=document.getElementById('hr-ex-fields-'+cid);
  var btn=document.getElementById('hr-ex-toggle-'+cid);
  if(!el) return;
  var isHidden=el.style.display==='none'||!el.style.display;
  el.style.display=isHidden?'block':'none';
  if(btn) btn.textContent=isHidden?'- Hide examples':'+ Add examples';
}

const PROXY_CODE = `// ═══════════════════════════════════════════════════

const PARALLEL_BASE  = 'https://api.parallel.ai';
const CRUSTDATA_BASE = 'https://api.crustdata.com';
const OPENAI_BASE    = 'https://api.openai.com';
const SMARTLEAD_BASE = 'https://server.smartlead.ai';
const AUTOBOUND_BASE = 'https://api.autobound.ai';

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-API-KEY,Authorization,Accept,Cache-Control,x-api-version,api-key',
};

function corsHeaders(extra = {}) {
  return { ...CORS, ...extra };
}

export default {
  async fetch(request, env) {
    const url    = new URL(request.url);
    const path   = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    if (method === 'PUT' && path.startsWith('/kv/')) {
      try {
        const body = await request.text();
        await env.DMAND_KV.put(path.slice(4), body);
        return new Response(JSON.stringify({ ok: true }), { status: 200, headers: corsHeaders({ 'Content-Type': 'application/json' }) });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders({ 'Content-Type': 'application/json' }) });
      }
    }

    if (method === 'GET' && path.startsWith('/kv/')) {
      try {
        const val = await env.DMAND_KV.get(path.slice(4));
        if (val === null) return new Response(JSON.stringify({ error: 'not_found' }), { status: 404, headers: corsHeaders({ 'Content-Type': 'application/json' }) });
        return new Response(val, { status: 200, headers: corsHeaders({ 'Content-Type': 'application/json' }) });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders({ 'Content-Type': 'application/json' }) });
      }
    }

    const bodyBytes = ['POST','PUT','PATCH'].includes(method) ? await request.arrayBuffer() : null;
    let target, headers;

    if (path.startsWith('/crustdata/')) {
      target  = CRUSTDATA_BASE + path.slice(10) + url.search;
      const k = request.headers.get('Authorization')?.replace('Bearer ','') || '';
      const v = request.headers.get('x-api-version') || '';
      headers = { 'Authorization': 'Bearer ' + k, 'Content-Type': 'application/json', 'Accept': 'application/json', ...(v ? {'x-api-version': v} : {}) };

    } else if (path.startsWith('/openai/')) {
      target  = OPENAI_BASE + path.slice(7) + url.search;
      headers = { 'Authorization': request.headers.get('Authorization') || '', 'Content-Type': 'application/json' };

    } else if (path.startsWith('/smartlead/')) {
      target  = SMARTLEAD_BASE + path.slice(10) + url.search;
      headers = { 'Content-Type': 'application/json' };

    } else if (path.startsWith('/apollo/')) {
      const apolloKey = request.headers.get('X-Api-Key') || '';
      const apolloUrl = new URL('https://api.apollo.io' + path.slice(8) + url.search);
      if (apolloKey) apolloUrl.searchParams.set('api_key', apolloKey);
      target  = apolloUrl.toString();
      headers = { 'Content-Type': 'application/json', 'Accept': 'application/json', 'Origin': 'https://app.apollo.io', 'Referer': 'https://app.apollo.io/' };

    } else if (path.startsWith('/autobound/')) {
      target  = AUTOBOUND_BASE + path.slice(10) + url.search;
      const k = request.headers.get('X-API-KEY') || request.headers.get('Authorization') || '';
      headers = { 'X-API-KEY': k, 'Content-Type': 'application/json', 'Accept': 'application/json' };

    } else if (path.startsWith('/fullenrich/')) {
      target  = 'https://app.fullenrich.com' + path.slice(11) + url.search;
      const k = request.headers.get('api-key') || request.headers.get('Authorization') || '';
      headers = { 'api-key': k, 'Content-Type': 'application/json', 'Accept': 'application/json' };

    } else if (path.startsWith('/heyreach/')) {
      target  = 'https://api.heyreach.io' + path.slice(9) + url.search;
      headers = { 'X-API-KEY': request.headers.get('X-API-KEY') || '', 'Content-Type': 'application/json', 'Accept': 'application/json' };

    } else {
      const k = request.headers.get('x-api-key') || '';
      target  = PARALLEL_BASE + path + url.search;
      headers = { 'x-api-key': k, 'Authorization': 'Bearer ' + k, 'Content-Type': 'application/json', 'Accept': 'application/json' };
    }

    try {
      const resp     = await fetch(target, { method, headers, body: bodyBytes });
      const respBody = await resp.arrayBuffer();
      return new Response(respBody, {
        status: resp.status,
        headers: corsHeaders({ 'Content-Type': resp.headers.get('Content-Type') || 'application/json' }),
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 503,
        headers: corsHeaders({ 'Content-Type': 'application/json' }),
      });
    }
  }
};
`;
function initProxyCodeBlock(){
  const el=document.getElementById('proxy-code-block');
  if(el) el.textContent=PROXY_CODE;
}
function copyProxyCode(){
  navigator.clipboard.writeText(PROXY_CODE).then(()=>{
    const s=document.getElementById('proxy-copy-status');
    if(s){s.textContent='Copied ✓';setTimeout(()=>s.textContent='',2500);}
  });
}

document.addEventListener('DOMContentLoaded',function(){
  try{init();initProxyCodeBlock();}
  catch(e){
    var l=document.getElementById('app-loader');
    if(l)l.style.display='none';
    console.error('Init error:',e);
    document.body.insertAdjacentHTML('beforeend','<div style="position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:#111;border:2px solid #f87171;padding:20px 28px;border-radius:12px;color:#f87171;font-family:monospace;z-index:9999;max-width:80vw"><b>Startup error:</b><br>'+e.message+'</div>');
  }
});
