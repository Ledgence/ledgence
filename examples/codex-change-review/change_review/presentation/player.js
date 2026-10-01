/* Self-contained explanatory replay of saved demo evidence. MIT. */
(() => {
  'use strict';
  const root = document.getElementById('demo');
  const data = JSON.parse(document.getElementById('demo-data').textContent);
  const graph = document.getElementById('workflow');
  const svg = document.getElementById('connections');
  const reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
  const nodes = Object.fromEntries(Array.from(graph.querySelectorAll('[data-node]'), el => [el.dataset.node, el]));
  const pass = {tests:data.tests.passed, review:data.review.verdict === 'approve', note:true,
    compare:data.comparison.cases.every(row => row.after.error === null && row.after.value === row.expected_pages)};
  const eligible = pass.tests && pass.review && pass.compare;
  const sample = [data.candidate,data.review,data.note].some(item => item.execution.cli_version.includes('fixture'));
  const at100 = data.comparison.cases.find(row => row.item_count === 100);
  const duration = [3600,3300,7000,3700,4700,5200];
  const outcome = data.status;
  const steps = [
    {name:'An agent proposes the fix', body:'Codex receives the bug and the small module. It returns a candidate that Ledgence can track through every check.', fact:sample ? 'Sample agent output · saved workflow evidence' : 'Codex output · saved with the candidate', code:'fix'},
    {name:'One version to check', body:'The candidate is ready to check. Every branch receives exactly the same candidate, so their results describe the same code.', fact:'One candidate shared across all three branches', code:'fix'},
    {name:'Split the work. Keep moving.', body:'Tests, an independent code review and a draft note run in separate branches. The parent also measures the behavior while those checks are in progress.', fact:data.tests.total + ' predefined tests · 3 independent branches', code:'fork'},
    {name:eligible ? 'Bring the evidence together' : 'The checks found a problem', body:eligible ? 'The next entrypoint receives every branch result. Tests, review and the measured comparison must agree before asking for approval.' : 'Ledgence preserves the results and stops before approval. A failed check is evidence to inspect, not a successful change.', fact:'100 documents · before: ' + at100.before.value + ' pages · after: ' + (at100.after.error ? 'error' : at100.after.value + ' page' + (at100.after.value === 1 ? '' : 's')), code:'fork'},
    {name:eligible ? 'Pause for a decision' : 'No approval requested', body:eligible ? 'The workflow saves its review packet and waits. It resumes at on_decision when the matching approval event arrives, or when the deadline expires.' : 'The candidate needs changes. The workflow does not ask for approval or publish it.', fact:eligible ? 'Durable wait · no parent invocation slot held' : 'The complete evidence remains available', code:'wait'},
    {name:outcome === 'approved' ? 'The exact change is approved' : outcome === 'waiting_for_approval' ? 'Ready for your decision' : outcome === 'rejected' ? 'The candidate was rejected' : outcome === 'expired' ? 'The decision window expired' : 'Ready for another attempt', body:outcome === 'approved' ? 'The recorded decision is bound to this candidate. Ledgence resumes the workflow and keeps its evidence together. Nothing is merged or deployed by default.' : outcome === 'waiting_for_approval' ? 'This saved run has no decision yet. The replay ends here; approval still needs the real companion command.' : 'The saved outcome stays explicit. This replay does not change it, submit work or send an approval.', fact:outcome === 'approved' ? 'Approval recorded · candidate unchanged' : 'Saved outcome: ' + outcome.replaceAll('_',' '), code:'wait'},
  ];
  const snippets = {
    fix:{text:data.snippets.fix, note:'The actual candidate diff in this saved run.'},
    fork:{text:data.snippets.fork, note:'The branches list contains the tests, review and draft-note workflows.'},
    wait:{text:data.snippets.wait, note:'Approval carries the workflow ID and the exact candidate SHA-256.'},
  };
  let stage = 0, elapsed = 0, playing = !reduced.matches, last = 0, frame = 0;
  let shownCode = '', selectedNode = '', rendered = '', edgePaths = [], flowDots = [];
  const pause = document.getElementById('play-pause');
  function showCode(name) {
    if (shownCode === name) return;
    shownCode = name;
    document.getElementById('code').textContent = snippets[name].text;
    document.getElementById('code-note').textContent = snippets[name].note;
    root.querySelectorAll('[data-code]').forEach(button => button.setAttribute('aria-pressed',String(button.dataset.code === name)));
  }
  function stateOf() {
    const states = {propose:stage === 0 ? 'running':'done',tests:'idle',review:'idle',note:'idle',compare:'idle',join:'idle',decide:'idle'};
    if (stage >= 2) {
      const offsets = {tests:.32,review:.8,note:.62,compare:.43};
      Object.keys(offsets).forEach(id => states[id] = stage > 2 || elapsed / duration[2] >= offsets[id] ? (pass[id] ? 'done':'failed'):'running');
    }
    if (stage >= 3) states.join = stage === 3 && elapsed / duration[3] < .45 ? 'running':(eligible ? 'done':'failed');
    if (stage >= 4 && eligible) states.decide = stage === 4 || outcome === 'waiting_for_approval' ? 'waiting':outcome === 'approved' ? 'done':'failed';
    return states;
  }
  function meta(id,state) {
    if (state === 'idle') return id === 'decide' && !eligible && stage >= 4 ? 'Not requested':'Waiting';
    if (state === 'running') return id === 'compare' ? 'Measure behavior':id === 'propose' ? 'Preparing code':'Running';
    if (state === 'waiting') return 'Awaiting decision';
    if (id === 'propose') return 'Candidate ready';
    if (id === 'tests') return state === 'done' ? data.tests.total + '/' + data.tests.total + ' passed':'Needs changes';
    if (id === 'review') return state === 'done' ? 'Review passed':'Changes requested';
    if (id === 'note') return 'Draft ready';
    if (id === 'compare') return state === 'done' ? '100 results · 1 page':'Mismatch found';
    if (id === 'join') return eligible ? 'Evidence ready':'Needs changes';
    return outcome === 'approved' ? 'Decision recorded':outcome === 'expired' ? 'Expired':'Rejected';
  }
  function render() {
    const states = stateOf();
    const signature = [stage,playing,selectedNode,...Object.values(states)].join('|');
    if(signature === rendered) return;
    rendered = signature;
    Object.entries(nodes).forEach(([id,node]) => {
      node.dataset.state = states[id];
      node.classList.toggle('selected',selectedNode === id);
      node.setAttribute('aria-pressed',String(selectedNode === id));
      node.querySelector('.node-status').textContent = meta(id,states[id]);
    });
    root.classList.toggle('is-paused',!playing);
    pause.textContent = playing ? 'Ⅱ Pause':'▶ Play';
    pause.setAttribute('aria-label',playing ? 'Pause demo playback':'Play demo playback');
    pause.setAttribute('aria-pressed',String(playing));
    document.getElementById('step-label').textContent = String(stage+1).padStart(2,'0') + ' / ' + steps.length + ' · Demo playback';
    document.getElementById('focus-title').textContent = steps[stage].name;
    document.getElementById('focus-body').textContent = steps[stage].body;
    document.getElementById('focus-fact').textContent = steps[stage].fact;
    const entry = ['start','check_candidate','check_candidate','prepare_review','await_decision',data.decision ? 'on_decision → finish':'await_decision'];
    document.getElementById('entrypoint').textContent = entry[stage];
    if(selectedNode) inspectNode(selectedNode);
    flowDots.forEach(edge=>{if(!playing || states[edge.to] !== 'running') edge.dot.setAttribute('opacity','0');});
    edgePaths.forEach(edge => {
      const target = states[edge.to];
      edge.path.setAttribute('class','edge' + (edge.fork ? ' fork':'') + (target === 'running' || target === 'waiting' ? ' active': target === 'done' ? ' done':''));
    });
  }
  function inspectNode(id) {
    const durationText = report => 'Observed ' + Math.max(0,report.finished_at_ms-report.started_at_ms) + ' ms in this saved run';
    const inspections = {
      propose:{title:'One candidate to inspect',body:data.candidate.summary,fact:'The exact diff is shown alongside this step',entry:'check_candidate'},
      tests:{title:'Fixed tests, actual results',body:data.tests.passed ? 'All six predefined regression tests passed, including empty results, exact pages and invalid input.' : 'The predefined regression suite found failures. See the evidence below for the complete output.',fact:(data.tests.total-data.tests.failures-data.tests.errors) + '/' + data.tests.total + ' passed · ' + durationText(data.tests),entry:'run_tests'},
      review:{title:'An independent code review',body:data.review.summary,fact:(pass.review ? 'Review passed':'Changes requested') + ' · ' + durationText(data.review),entry:'review_code'},
      note:{title:data.note.title,body:data.note.body,fact:'Draft note · ' + durationText(data.note),entry:'draft_note'},
      compare:{title:'Measure the customer-visible result',body:'Both versions ran with 99, 100 and 101 documents. The expected page counts are 1, 1 and 2.',fact:steps[3].fact,entry:'check_candidate · local step'},
      join:{title:eligible ? 'All evidence is available':'A check needs attention',body:steps[3].body,fact:'Three branch results plus the local comparison',entry:'prepare_review'},
      decide:{title:steps[5].name,body:steps[5].body,fact:steps[5].fact,entry:data.decision ? 'on_decision → finish':'await_decision'},
    };
    const item=inspections[id];
    document.getElementById('step-label').textContent='Inspecting saved result';
    document.getElementById('focus-title').textContent=item.title;
    document.getElementById('focus-body').textContent=item.body;
    document.getElementById('focus-fact').textContent=item.fact;
    document.getElementById('entrypoint').textContent=item.entry;
  }
  function createSvg(tag,attributes) {
    const el = document.createElementNS('http://www.w3.org/2000/svg',tag);
    Object.entries(attributes).forEach(([key,value])=>el.setAttribute(key,String(value)));
    svg.appendChild(el);
    return el;
  }
  function wire() {
    const box = graph.getBoundingClientRect();
    const mobile = window.innerWidth <= 900;
    svg.setAttribute('viewBox','0 0 ' + box.width + ' ' + box.height);
    svg.replaceChildren(); edgePaths = []; flowDots = []; rendered = '';
    const rect = id => {
      const r = nodes[id].getBoundingClientRect();
      return {x:r.left-box.left,y:r.top-box.top,w:r.width,h:r.height,cx:r.left-box.left+r.width/2,cy:r.top-box.top+r.height/2};
    };
    const pos = Object.fromEntries(Object.keys(nodes).map(id=>[id,rect(id)]));
    function edge(from,to,d,fork) {
      const path=createSvg('path',{d,class:'edge' + (fork ? ' fork':'')});
      edgePaths.push({from,to,path,fork});
      flowDots.push({to,path,length:path.getTotalLength(),dot:createSvg('circle',{r:2.6,class:'flow-dot',opacity:0})});
    }
    const a=pos.propose, j=pos.join, decision=pos.decide, c=pos.compare;
    if (!mobile) {
      const forkX=a.x+a.w+19, mergeX=j.x-18;
      for (const id of ['tests','review','note']) {
        const b=pos[id];
        edge('propose',id,'M '+(a.x+a.w)+' '+a.cy+' H '+forkX+' V '+b.cy+' H '+b.x,true);
        edge(id,'join','M '+(b.x+b.w)+' '+b.cy+' H '+mergeX+' V '+j.cy+' H '+j.x,true);
      }
      edge('propose','compare','M '+(a.x+a.w)+' '+a.cy+' H '+forkX+' V '+c.cy+' H '+c.x,false);
      edge('compare','join','M '+(c.x+c.w)+' '+c.cy+' H '+mergeX+' V '+j.cy+' H '+j.x,false);
      edge('join','decide','M '+(j.x+j.w)+' '+j.cy+' H '+decision.x,false);
      createSvg('circle',{cx:forkX,cy:a.cy,r:4,class:'junction'});
      const label=createSvg('text',{x:forkX-9,y:pos.tests.cy-17,class:'junction-label'});label.textContent='FORK';
    } else {
      const splitY=a.y+a.h+15, mergeY=j.y-16;
      for (const id of ['tests','review','note']) {
        const b=pos[id];
        edge('propose',id,'M '+a.cx+' '+(a.y+a.h)+' V '+splitY+' H '+b.cx+' V '+b.y,true);
        const lane=id === 'tests' ? b.x+8:id === 'note' ? b.x+b.w-8:b.x+b.w-4;
        edge(id,'join','M '+b.cx+' '+(b.y+b.h)+' V '+(b.y+b.h+12)+' H '+lane+' V '+mergeY+' H '+j.cx+' V '+j.y,true);
      }
      edge('propose','compare','M '+a.x+' '+a.cy+' H 7 V '+c.cy+' H '+c.x,false);
      edge('compare','join','M '+c.cx+' '+(c.y+c.h)+' V '+j.y,false);
      edge('join','decide','M '+j.cx+' '+(j.y+j.h)+' V '+decision.y,false);
      createSvg('circle',{cx:a.cx,cy:splitY,r:4,class:'junction'});
    }
    render();
  }
  function jump(index,manual) {
    stage=(index+steps.length)%steps.length;elapsed=0;last=0;
    if(manual) playing=false;
    showCode(steps[stage].code);render();
  }
  pause.addEventListener('click',()=>{playing=!playing;last=0;selectedNode='';render();});
  document.getElementById('previous').addEventListener('click',()=>{selectedNode='';jump(stage-1,true);});
  document.getElementById('next').addEventListener('click',()=>{selectedNode='';jump(stage+1,true);});
  document.getElementById('restart').addEventListener('click',()=>{selectedNode='';playing=!reduced.matches;jump(0,false);});
  Object.entries(nodes).forEach(([id,node])=>node.addEventListener('click',()=>{selectedNode=id;jump(id === 'propose' ? 1:id === 'decide' ? 5:Number(node.dataset.step),true);elapsed=duration[stage]-1;render();}));
  root.querySelectorAll('[data-code]').forEach(button=>button.addEventListener('click',()=>{playing=false;showCode(button.dataset.code);render();}));
  root.querySelector('.evidence-link').addEventListener('click',()=>{document.getElementById('evidence').open=true;playing=false;render();});
  document.addEventListener('visibilitychange',()=>{last=0;});
  reduced.addEventListener('change',()=>{if(reduced.matches)playing=false;last=0;render();});
  function tick(now) {
    if(playing && !document.hidden) {
      if(last) elapsed+=Math.min(now-last,100);
      if(elapsed>=duration[stage]) {selectedNode='';jump(stage+1,false);}
      last=now;render();
      const states=stateOf();
      flowDots.forEach((edge,index)=>{
        const active=states[edge.to] === 'running' && !reduced.matches;
        edge.dot.setAttribute('opacity',active ? '.9':'0');
        if(active){const p=edge.path.getPointAtLength(((now/1900+index*.13)%1)*edge.length);edge.dot.setAttribute('cx',p.x);edge.dot.setAttribute('cy',p.y);}
      });
    } else last=0;
    frame=requestAnimationFrame(tick);
  }
  window.addEventListener('pagehide',()=>cancelAnimationFrame(frame));
  window.addEventListener('pageshow',event=>{if(event.persisted){last=0;frame=requestAnimationFrame(tick);}});
  new ResizeObserver(wire).observe(graph);
  showCode('fix');wire();render();frame=requestAnimationFrame(tick);
})();
