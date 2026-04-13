
// ── Dmand Poll Worker — runs on background thread ──
// Receives: {type:'poll', signals:[], proxy:'', apiKey:'', lookback:''}
// Sends back: {type:'result', signalId:'', newEvents:[], completions:0}
// Sends back: {type:'done', totalNew:0, totalComp:0}

self.onmessage = async function(e) {
  const {type, signals, proxy, apiKey, lookback} = e.data;
  if(type !== 'poll') return;

  let totalNew = 0, totalComp = 0;

  for(const sig of signals){
    try {
      const url = proxy + '/v1alpha/monitors/' + sig.monitor_id + '/events?lookback=' + (lookback||'3d');
      const resp = await fetch(url, {
        headers: {
          'x-api-key': apiKey,
          'Authorization': 'Bearer ' + apiKey,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        signal: AbortSignal.timeout(25000)
      });

      if(!resp.ok){
        self.postMessage({type:'error', signalId:sig.id, status:resp.status});
        continue;
      }

      const data = await resp.json();
      const rawEvents = Array.isArray(data) ? data : (Array.isArray(data.events) ? data.events : []);

      const newEvents = [];
      let completions = 0;

      for(const ev of rawEvents){
        const evType = (ev.type||'').toLowerCase();
        if(evType === 'completion' || (ev.monitor_ts && !ev.output && evType !== 'event')){
          completions++;
          continue;
        }
        if(evType === 'error'){ completions++; continue; }
        if(evType !== 'event') continue;

        // Build event ID
        const eid = ev.event_id || ev.event_group_id || 
          ('ev_' + sig.id + '_' + (ev.output||'').slice(0,32).replace(/\W/g,''));

        newEvents.push({
          event_id: eid,
          type: 'event',
          signal_id: sig.id,
          signal_name: sig.name,
          category: sig.category||'',
          output: ev.output || ev.description || ev.content || '',
          company_name: ev.company_name || ev.org_name || '',
          company_domain: ev.company_domain || ev.domain || '',
          company_linkedin_url: ev.company_linkedin_url || ev.linkedin_url || '',
          event_date: ev.event_date || ev.date || '',
          fetched_at: new Date().toISOString(),
          enrichment: null
        });
      }

      totalNew += newEvents.length;
      totalComp += completions;

      // Send results for this signal back to main thread
      self.postMessage({
        type: 'signal_result',
        signalId: sig.id,
        signalName: sig.name,
        newEvents,
        completions,
        rawCount: rawEvents.length
      });

      // Tiny yield between monitors
      await new Promise(r => setTimeout(r, 50));

    } catch(err) {
      self.postMessage({type:'error', signalId:sig.id, message:err.message});
    }
  }

  self.postMessage({type:'done', totalNew, totalComp});
};
