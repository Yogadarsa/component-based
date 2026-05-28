document.addEventListener('DOMContentLoaded', () => {
    
    const workflowListEl = document.getElementById('workflow-list');
    const executionPanel = document.getElementById('execution-panel');
    const emptyState = document.getElementById('empty-state');
    const wfTitle = document.getElementById('wf-title');
    const wfDesc = document.getElementById('wf-desc');
    const btnRun = document.getElementById('btn-run');
    
    const resultsContainer = document.getElementById('results-container');
    const resStatus = document.getElementById('res-status');
    const resDuration = document.getElementById('res-duration');
    const resNodes = document.getElementById('res-nodes');
    const terminalBody = document.getElementById('terminal-body');
    const resJson = document.getElementById('res-json');
    
    let currentWorkflow = null;

    // Fetch workflows
    fetch('/api/workflows')
        .then(res => res.json())
        .then(data => {
            workflowListEl.innerHTML = '';
            data.workflows.forEach(wf => {
                const card = document.createElement('div');
                card.className = 'workflow-card';
                card.innerHTML = `
                    <div class="workflow-name">${wf.title}</div>
                    <div class="workflow-desc">${wf.description}</div>
                `;
                card.addEventListener('click', () => selectWorkflow(wf, card));
                workflowListEl.appendChild(card);
            });
        })
        .catch(err => {
            workflowListEl.innerHTML = `<div style="color: var(--error);">Failed to load workflows</div>`;
        });

    function selectWorkflow(wf, cardElement) {
        currentWorkflow = wf;
        
        // Update active class
        document.querySelectorAll('.workflow-card').forEach(c => c.classList.remove('active'));
        cardElement.classList.add('active');

        // Show execution panel
        emptyState.style.display = 'none';
        executionPanel.style.display = 'block';
        resultsContainer.classList.add('hidden');
        
        wfTitle.textContent = wf.title;
        wfDesc.textContent = wf.description;
    }

    btnRun.addEventListener('click', () => {
        if (!currentWorkflow) return;

        // Reset UI
        btnRun.disabled = true;
        btnRun.innerHTML = `<i data-lucide="loader-2" class="icon spin"></i> Running...`;
        lucide.createIcons();
        resultsContainer.classList.remove('hidden');
        
        resStatus.textContent = 'RUNNING...';
        resStatus.className = 'metric-value status-PENDING';
        resDuration.textContent = '--';
        resNodes.textContent = '--';
        terminalBody.innerHTML = '';
        resJson.textContent = 'Executing...';

        // Fake terminal typing effect for realism
        addLog({ event_type: "SYSTEM", node_id: "Engine", message: `Initializing DAG execution...` });

        fetch(`/api/run/${currentWorkflow.id}`, { method: 'POST' })
            .then(res => res.json())
            .then(data => {
                // Render results
                resStatus.textContent = data.status;
                resStatus.className = `metric-value status-${data.status}`;
                resDuration.textContent = `${data.duration_seconds.toFixed(3)}s`;
                resNodes.textContent = Object.keys(data.node_results).length;
                resJson.textContent = JSON.stringify(data.node_results, null, 2);

                // Replay events with a slight delay to look like real-time
                terminalBody.innerHTML = '';
                let delay = 0;
                const totalDur = data.duration_seconds * 1000;
                const step = Math.min(50, totalDur / (data.events.length || 1));
                
                data.events.forEach((evt, i) => {
                    setTimeout(() => {
                        addLog(evt);
                        if (i === data.events.length - 1) {
                            btnRun.disabled = false;
                            btnRun.innerHTML = `<i data-lucide="play" class="icon"></i> Run Workflow`;
                            lucide.createIcons();
                        }
                    }, delay);
                    delay += step;
                });

                if(data.events.length === 0) {
                     btnRun.disabled = false;
                     btnRun.innerHTML = `<i data-lucide="play" class="icon"></i> Run Workflow`;
                     lucide.createIcons();
                }
            })
            .catch(err => {
                resStatus.textContent = 'ERROR';
                resStatus.className = 'metric-value status-FAILED';
                addLog({ event_type: "SYSTEM_ERROR", node_id: "System", message: err.toString() });
                btnRun.disabled = false;
                btnRun.innerHTML = `<i data-lucide="play" class="icon"></i> Run Workflow`;
                lucide.createIcons();
            });
    });

    function addLog(evt) {
        const line = document.createElement('div');
        line.className = 'log-entry';
        
        const time = new Date().toISOString().split('T')[1].substring(0, 12);
        const eventTypeClass = `evt-${evt.event_type}`;
        
        let msg = `[${time}] `;
        
        if (evt.event_type === 'SYSTEM' || evt.event_type === 'SYSTEM_ERROR') {
            msg += `<span style="color: #94a3b8;">[System] ${evt.message}</span>`;
        } else {
            msg += `<span class="${eventTypeClass}">[${evt.event_type}]</span> `;
            msg += `Node='<span style="color: #fff;">${evt.node_id || 'workflow'}</span>'`;
            if (evt.data && Object.keys(evt.data).length > 0) {
                // Stringify data simply
                const dataStr = Object.entries(evt.data)
                    .map(([k,v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`)
                    .join(', ');
                msg += ` <span style="color: #64748b;">${dataStr}</span>`;
            }
        }
        
        line.innerHTML = msg;
        terminalBody.appendChild(line);
        terminalBody.scrollTop = terminalBody.scrollHeight;
    }
});
