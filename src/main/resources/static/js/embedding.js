async function commitTextKnowledge() {
    const text = document.getElementById("plainTextDoc").value;
    const status = document.getElementById("textStatus");
    if(!text.trim()) return;

    status.innerText = "벡터 연산 중...";
    try {
        const res = await fetch('/api/rag/ingest', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ content: text })
        });
        status.innerText = await res.text();
        document.getElementById("plainTextDoc").value = "";
    } catch(e) { status.innerText = "실패: " + e; }
}

async function commitFileKnowledge() {
    const fileInput = document.getElementById("binaryFileDoc");
    const status = document.getElementById("fileStatus");
    if(fileInput.files.length === 0) return;

    const fd = new FormData();
    fd.append("file", fileInput.files[0]);
    status.innerText = "Tika 파싱 처리 프로세스 가동 중...";

    try {
        const res = await fetch('/api/rag/ingest-file', { method: 'POST', body: fd });
        status.innerText = await res.text();
        fileInput.value = "";
    } catch(e) { status.innerText = "실패: " + e; }
}

async function commitShoeKnowledge() {
    const status = document.getElementById("shoeStatus");
    const payload = {
        brand: document.getElementById("brand").value,
        model: document.getElementById("model").value,
        footShape: document.getElementById("footShape").value,
        footShapeSpec: document.getElementById("footShapeSpec").value,
        wallSize: document.getElementById("wallSize").value,
        padType: document.getElementById("padType").value,
        toeBozSize: document.getElementById("toeBozSize").value,
        heelCupSize: document.getElementById("heelCupSize").value,
        heepCupHardness: document.getElementById("heepCupHardness").value,
        mainUses: document.getElementById("mainUses").value,
        description: document.getElementById("desc").value
    };

    status.innerText = "인덱싱 반영 중...";
    try {
        const res = await fetch('/api/rag/ingest-shoe', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        status.innerText = await res.text();
    } catch(e) { status.innerText = "저장 실패: " + e; }
}